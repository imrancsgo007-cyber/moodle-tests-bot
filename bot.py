# ============================================================
#  project_kbgu.py — бот для сбора тестов Moodle
#  Всё в одном файле: модели, репозиторий, парсер, бот
# ============================================================

import asyncio
import logging
import re
from datetime import datetime, timedelta

from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from aiogram.types import BotCommand, BotCommandScopeDefault

from sqlalchemy import (
    Column, String, Integer, DateTime, Index, UniqueConstraint, event,
    select, delete, update,
)
from sqlalchemy.ext.asyncio import (
    create_async_engine, async_sessionmaker, AsyncSession,
)
from sqlalchemy.orm import declarative_base

from playwright.async_api import async_playwright

from bot_token2 import BOT_TOKEN

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


# ============================================================
#  МОДЕЛИ
# ============================================================

Base = declarative_base()

async def set_commands():
    commands = [
        BotCommand(command="start", description="Регистрация"),
        BotCommand(command="menu", description="Показать меню"),
        BotCommand(command="events", description="Мои события"),
        BotCommand(command="hidden", description="Скрытые события"),
        BotCommand(command="profiles", description="Управление профилями"),
        BotCommand(command="cancel", description="Отменить текущее действие"),
    ]
    await bot.set_my_commands(commands, BotCommandScopeDefault())

class User(Base):
    __tablename__ = 'users'
    id = Column(Integer, primary_key=True, autoincrement=True)
    telegram_id = Column(Integer, unique=True, nullable=False)
    active_profile_id = Column(Integer, nullable=True)
    created_at = Column(DateTime, default=datetime.now)


class MoodleProfile(Base):
    __tablename__ = 'moodle_profiles'
    id = Column(Integer, primary_key=True, autoincrement=True)
    telegram_id = Column(Integer, nullable=False, index=True)
    label = Column(String, nullable=False)
    login = Column(String, nullable=False)
    password = Column(String, nullable=False)
    institute = Column(String, nullable=False)
    created_at = Column(DateTime, default=datetime.now)
    __table_args__ = (
        UniqueConstraint('telegram_id', 'login', name='uq_profile_user_login'),
    )


class TestEvent(Base):
    __tablename__ = 'tests'
    id = Column(Integer, primary_key=True, autoincrement=True)
    profile_id = Column(Integer, nullable=False, index=True)
    telegram_id = Column(Integer, nullable=False, index=True)
    url = Column(String, nullable=False)
    course_name = Column(String, default='')
    test_name = Column(String, default='')
    open_datetime = Column(String, default='')
    close_datetime = Column(String, default='')
    open_dt_parsed = Column(DateTime, nullable=True)
    close_dt_parsed = Column(DateTime, nullable=True)
    status = Column(String, default='неизвестно')
    is_hidden = Column(Integer, default=0, nullable=False)
    parsed_at = Column(DateTime, default=datetime.now)
    __table_args__ = (
        UniqueConstraint('profile_id', 'url', name='uq_profile_url'),
        Index('ix_tests_user_hidden_parsed', 'telegram_id', 'is_hidden', 'parsed_at'),
        Index('ix_tests_close_dt', 'close_dt_parsed'),
    )


class ParseResult(Base):
    __tablename__ = 'parse_results'
    id = Column(Integer, primary_key=True, autoincrement=True)
    telegram_id = Column(Integer, nullable=False, index=True)
    institute = Column(String, default='')
    result_count = Column(Integer, default=0)
    result_data = Column(String, default='{}')
    created_at = Column(DateTime, default=datetime.now)


# ============================================================
#  ENGINE + SESSION
# ============================================================

DATABASE_URL = 'sqlite+aiosqlite:///bot_database.db'

engine = create_async_engine(
    DATABASE_URL,
    echo=False,
    connect_args={'timeout': 30},
)


@event.listens_for(engine.sync_engine, 'connect')
def _set_sqlite_pragma(dbapi_conn, _):
    cur = dbapi_conn.cursor()
    cur.execute('PRAGMA journal_mode=WAL')
    cur.execute('PRAGMA synchronous=NORMAL')
    cur.execute('PRAGMA busy_timeout=30000')
    cur.execute('PRAGMA foreign_keys=ON')
    cur.close()


Session = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)


async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


# ============================================================
#  РЕПОЗИТОРИЙ
# ============================================================

async def get_user(telegram_id: int) -> User | None:
    async with Session() as s:
        return await s.scalar(select(User).where(User.telegram_id == telegram_id))


async def ensure_user(telegram_id: int) -> User:
    async with Session() as s:
        u = await s.scalar(select(User).where(User.telegram_id == telegram_id))
        if not u:
            u = User(telegram_id=telegram_id)
            s.add(u)
            await s.commit()
            await s.refresh(u)
        return u


async def set_active_profile(telegram_id: int, profile_id: int | None):
    async with Session() as s:
        await s.execute(
            update(User).where(User.telegram_id == telegram_id)
            .values(active_profile_id=profile_id)
        )
        await s.commit()


async def get_active_profile(telegram_id: int) -> MoodleProfile | None:
    u = await get_user(telegram_id)
    if not u or not u.active_profile_id:
        return None
    return await get_profile(u.active_profile_id)


async def list_profiles(telegram_id: int) -> list[MoodleProfile]:
    async with Session() as s:
        res = await s.scalars(
            select(MoodleProfile).where(MoodleProfile.telegram_id == telegram_id)
            .order_by(MoodleProfile.id)
        )
        return list(res)


async def get_profile(profile_id: int) -> MoodleProfile | None:
    async with Session() as s:
        return await s.get(MoodleProfile, profile_id)


async def add_profile(telegram_id: int, label: str, login: str, password: str, institute: str) -> MoodleProfile:
    async with Session() as s:
        p = MoodleProfile(
            telegram_id=telegram_id, label=label, login=login,
            password=password, institute=institute,
        )
        s.add(p)
        await s.commit()
        await s.refresh(p)
        return p


async def delete_profile(profile_id: int):
    async with Session() as s:
        await s.execute(delete(TestEvent).where(TestEvent.profile_id == profile_id))
        await s.execute(delete(MoodleProfile).where(MoodleProfile.id == profile_id))
        await s.commit()


async def save_event(data: dict, profile: MoodleProfile):
    async with Session() as s:
        existing = await s.scalar(
            select(TestEvent).where(
                TestEvent.profile_id == profile.id,
                TestEvent.url == data['url'],
            )
        )
        if existing:
            existing.course_name = data.get('course_name', existing.course_name)
            existing.test_name = data.get('test_name', existing.test_name)
            existing.open_datetime = data.get('open_date', '')
            existing.close_datetime = data.get('close_date', '')
            existing.open_dt_parsed = data.get('open_dt_parsed')
            existing.close_dt_parsed = data.get('close_dt_parsed')
            existing.status = data.get('status', existing.status)
        else:
            s.add(TestEvent(
                profile_id=profile.id,
                telegram_id=profile.telegram_id,
                url=data['url'],
                course_name=data.get('course_name', ''),
                test_name=data.get('test_name', ''),
                open_datetime=data.get('open_date', ''),
                close_datetime=data.get('close_date', ''),
                open_dt_parsed=data.get('open_dt_parsed'),
                close_dt_parsed=data.get('close_dt_parsed'),
                status=data.get('status', 'неизвестно'),
            ))
        await s.commit()


async def list_events(telegram_id: int, hidden: bool = False, limit: int = 20) -> list[TestEvent]:
    async with Session() as s:
        res = await s.scalars(
            select(TestEvent)
            .where(TestEvent.telegram_id == telegram_id, TestEvent.is_hidden == int(hidden))
            .order_by(TestEvent.parsed_at.desc())
            .limit(limit)
        )
        return list(res)


async def get_event(event_id: int, telegram_id: int) -> TestEvent | None:
    async with Session() as s:
        return await s.scalar(
            select(TestEvent).where(
                TestEvent.id == event_id, TestEvent.telegram_id == telegram_id,
            )
        )


async def set_hidden(event_id: int, telegram_id: int, hidden: bool):
    async with Session() as s:
        await s.execute(
            update(TestEvent)
            .where(TestEvent.id == event_id, TestEvent.telegram_id == telegram_id)
            .values(is_hidden=int(hidden))
        )
        await s.commit()


async def delete_expired(grace_days: int = 3) -> int:
    threshold = datetime.now() - timedelta(days=grace_days)
    async with Session() as s:
        res = await s.execute(
            delete(TestEvent).where(
                TestEvent.close_dt_parsed.is_not(None),
                TestEvent.close_dt_parsed < threshold,
                TestEvent.is_hidden == 0,
            )
        )
        await s.commit()
        return res.rowcount or 0


async def add_parse_result(telegram_id: int, institute: str, count: int):
    async with Session() as s:
        s.add(ParseResult(telegram_id=telegram_id, institute=institute, result_count=count))
        await s.commit()


async def list_parse_results(telegram_id: int, limit: int = 5):
    async with Session() as s:
        res = await s.scalars(
            select(ParseResult).where(ParseResult.telegram_id == telegram_id)
            .order_by(ParseResult.created_at.desc()).limit(limit)
        )
        return list(res)


async def list_all_users():
    async with Session() as s:
        res = await s.scalars(select(User).order_by(User.created_at.desc()))
        return list(res)


# ============================================================
#  ПАРСЕР
# ============================================================

# >>> ИЗМЕНЕНО: класс ошибки входа
class MoodleAuthError(Exception):
    """Неверный логин/пароль или выбран не тот институт."""
    pass


_MONTHS = {
    'january': 1, 'february': 2, 'march': 3, 'april': 4, 'may': 5, 'june': 6,
    'july': 7, 'august': 8, 'september': 9, 'october': 10, 'november': 11, 'december': 12,
}
_DATE_RE = re.compile(r'(?:\w+,\s*)?(\d{1,2})\s+(\w+)\s+(\d{4}),\s*(\d{1,2}):(\d{2})')
_RAW_DATE_RE = re.compile(r'(?:\w+,\s*)?\d{1,2}\s+\w+\s+\d{4},\s*\d{1,2}:\d{2}')


def parse_date(raw: str) -> datetime | None:
    m = _DATE_RE.search(raw)
    if not m:
        return None
    day, month_name, year, hour, minute = m.groups()
    month = _MONTHS.get(month_name.lower())
    if not month:
        return None
    try:
        return datetime(int(year), month, int(day), int(hour), int(minute))
    except ValueError:
        return None


_GLOBAL_PARSE_SEM = asyncio.Semaphore(2)


async def process_event(page, event: dict) -> dict | None:
    try:
        await page.goto(event['url'], timeout=15000)
        await page.wait_for_load_state("networkidle", timeout=20000)

        course_el = await page.query_selector('div.page-header-headings h1')
        if course_el:
            event['course_name'] = (await course_el.text_content()).strip()

        info_el = await page.query_selector('[data-region="activity-information"]')
        if info_el:
            name = await info_el.get_attribute('data-activityname')
            if name:
                event['test_name'] = name.strip()
        if not event.get('test_name'):
            h2_el = await page.query_selector('[role="main"] h2')
            if h2_el:
                event['test_name'] = (await h2_el.text_content()).strip()

        dates_section = await page.query_selector('[data-region="activity-dates"]')
        if dates_section:
            text = await dates_section.text_content()
            raw_dates = _RAW_DATE_RE.findall(text)
            if len(raw_dates) >= 1:
                event['open_date'] = raw_dates[0].strip()
                event['open_dt_parsed'] = parse_date(raw_dates[0])
            if len(raw_dates) >= 2:
                event['close_date'] = raw_dates[1].strip()
                event['close_dt_parsed'] = parse_date(raw_dates[1])

        now = datetime.now()
        o, c = event.get('open_dt_parsed'), event.get('close_dt_parsed')
        if o and c:
            if now < o:
                event['status'] = 'не открыт'
            elif now >= c:
                event['status'] = 'закрыт'
            elif now >= c - timedelta(days=1):
                event['status'] = 'тест закрывается'
            else:
                event['status'] = 'открыт'
        else:
            event['status'] = 'неизвестно'

        return event
    except Exception as e:
        log.warning("process_event failed for %s: %s", event.get('url'), e)
        return None


async def parse_moodle(profile: MoodleProfile, calendar_url: str) -> int:
    async with _GLOBAL_PARSE_SEM:
        async with async_playwright() as p:
            browser = await p.firefox.launch(headless=True)
            context = await browser.new_context()
            try:
                page = await context.new_page()
                await page.goto(calendar_url, timeout=20000)

                # >>> ИЗМЕНЕНО: проверка результата логина
                if await page.query_selector('input[name="username"]'):
                    await page.fill('input[name="username"]', profile.login)
                    await page.fill('input[name="password"]', profile.password)
                    await page.click('button[type="submit"]')
                    try:
                        await page.wait_for_load_state("networkidle", timeout=20000)
                    except Exception:
                        pass

                    # Если форма логина всё ещё на странице — креды неверные
                    if await page.query_selector('input[name="username"]'):
                        err_el = await page.query_selector(
                            '.loginerrors, .alert-danger, [role="alert"]'
                        )
                        err_text = (await err_el.text_content()).strip() if err_el else ""
                        log.warning(
                            "login failed for %s (%s): %s",
                            profile.login, profile.institute, err_text,
                        )
                        raise MoodleAuthError("Неверный логин или пароль")

                    if "calendar/view.php" not in page.url:
                        await page.goto(calendar_url, timeout=20000)

                # >>> ИЗМЕНЕНО: если календарь не открылся — вероятно, не тот институт
                try:
                    await page.wait_for_selector('.calendarwrapper', timeout=15000)
                except Exception:
                    raise MoodleAuthError(
                        f"Календарь не открылся. Проверьте, что выбран верный институт "
                        f"({profile.institute})."
                    )
                # <<< конец изменений

                urls, seen = [], set()
                for item in await page.query_selector_all('li[data-region="event-item"]'):
                    component = (await item.get_attribute('data-event-component')
                                 or await item.get_attribute('dava-event-component'))
                    if component != 'mod_quiz':
                        continue
                    a = await item.query_selector('a[data-action="view-event"]')
                    if not a:
                        continue
                    href = await a.get_attribute('href')
                    if href and href not in seen:
                        seen.add(href)
                        urls.append(href)
                await page.close()
                log.info("profile %s: %d urls", profile.id, len(urls))

                inner_sem = asyncio.Semaphore(5)

                async def worker(url: str):
                    async with inner_sem:
                        p2 = await context.new_page()
                        try:
                            ev = await process_event(p2, {'url': url})
                            if ev:
                                await save_event(ev, profile)
                                return True
                        finally:
                            await p2.close()
                    return False

                results = await asyncio.gather(
                    *(worker(u) for u in urls), return_exceptions=True,
                )
                ok = sum(1 for r in results if r is True)
                log.info("profile %s: saved %d/%d", profile.id, ok, len(urls))
                return ok
            finally:
                await browser.close()


async def cleanup_loop(interval_sec: int = 3600, grace_days: int = 3):
    while True:
        try:
            n = await delete_expired(grace_days=grace_days)
            if n:
                log.info("cleanup: deleted %d expired events", n)
        except Exception as e:
            log.exception("cleanup failed: %s", e)
        await asyncio.sleep(interval_sec)


# ============================================================
#  БОТ
# ============================================================

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

INSTITUTE_URLS = {
    "ИМиЕН":    "https://open.kbsu.ru:8033/moodle/calendar/view.php?view=month",
    "ИЭРИиИ": "https://open08.kbsu.ru:8033/moodle/calendar/view.php?view=month",
    "МА":      "https://open03.kbsu.ru:8033/moodle/calendar/view.php?view=month",
    "ИПЭиФ": "https://open07.kbsu.ru:8033/moodle/calendar/view.php?view=month",
    "СГИ": "https://open01.kbsu.ru:8033/moodle/calendar/view.php?view=month",
    "МК": "https://open06.kbsu.ru:8033/moodle/calendar/view.php?view=month",
    "Педколледж": "https://open05.kbsu.ru:8033/moodle/calendar/view.php?view=month",
    "ИСиЧЛХ": "https://open04.kbsu.ru:8033/moodle/calendar/view.php?view=month",
    "КИТиЭ": "https://open04.kbsu.ru:8033/moodle/calendar/view.php?view=month",
    "ИАСиД": "https://open02.kbsu.ru:8033/moodle/calendar/view.php?view=month"
}
institutes = list(INSTITUTE_URLS.keys())

ADMIN_IDS = [5240018166]


class Reg(StatesGroup):
    login = State()
    password = State()
    institute = State()
    label = State()


class AddProfile(StatesGroup):
    login = State()
    password = State()
    institute = State()
    label = State()


# ===== КЛАВИАТУРЫ =====

def institute_kb() -> InlineKeyboardMarkup:
    buttons = [[InlineKeyboardButton(text=i, callback_data=f"inst:{i}")] for i in institutes]
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def main_menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 Обновить данные", callback_data="menu:parse")],
        [InlineKeyboardButton(text="📅 Мои события", callback_data="menu:events")],
        [InlineKeyboardButton(text="🙈 Скрытые", callback_data="menu:hidden")],
        [InlineKeyboardButton(text="👤 Профили", callback_data="menu:profiles")],
        [InlineKeyboardButton(text="🕓 История", callback_data="menu:history")],
    ])


# ===== /start =====

@dp.message(Command("start"))
async def cmd_start(message: types.Message, state: FSMContext):
    await state.clear()
    await ensure_user(message.from_user.id)
    profiles = await list_profiles(message.from_user.id)

    if not profiles:
        await message.answer(
            "👋 Привет! Я бот для хранения твоих тестов Moodle.\n\n"
            "Давай создадим первый профиль.\nВведите ЛОГИН Moodle:"
        )
        await state.set_state(Reg.login)
        return

    active = await get_active_profile(message.from_user.id)
    active_text = f"\nАктивный профиль: <b>{active.label}</b> ({active.login})" if active else ""
    await message.answer(
        f"👋 С возвращением!{active_text}\n\nВыберите действие:",
        reply_markup=main_menu_kb(),
        parse_mode="HTML",
    )


# ===== /menu =====

@dp.message(Command("menu"))
async def cmd_menu(message: types.Message):
    await message.answer("Главное меню:", reply_markup=main_menu_kb())


# ===== /cancel =====

@dp.message(Command("cancel"))
async def cmd_cancel(message: types.Message, state: FSMContext):
    if await state.get_state() is None:
        await message.answer("❌ Нет активных операций.")
        return
    await state.clear()
    await message.answer("✅ Отменено. /menu — вернуться в меню.")


# ===== РЕГИСТРАЦИЯ ПЕРВОГО ПРОФИЛЯ =====

@dp.message(Reg.login)
async def reg_login(message: types.Message, state: FSMContext):
    if not message.text:
        await message.answer("Пришлите логин текстом")
        return
    await state.update_data(login=message.text.strip())
    await message.answer("Введите ПАРОЛЬ:")
    await state.set_state(Reg.password)


@dp.message(Reg.password)
async def reg_password(message: types.Message, state: FSMContext):
    if not message.text:
        await message.answer("Пришлите пароль текстом")
        return
    await state.update_data(password=message.text.strip())
    await message.answer("Выберите ИНСТИТУТ:", reply_markup=institute_kb())
    await state.set_state(Reg.institute)


@dp.callback_query(Reg.institute, F.data.startswith("inst:"))
async def reg_institute(callback: CallbackQuery, state: FSMContext):
    institute = callback.data.split(":", 1)[1]
    await state.update_data(institute=institute)
    await callback.answer()
    await callback.message.edit_text(
        f"🏫 Институт: {institute}\n\n"
        f"Придумайте название профиля (например, «Основной»):"
    )
    await state.set_state(Reg.label)


@dp.message(Reg.label)
async def reg_label(message: types.Message, state: FSMContext):
    if not message.text:
        await message.answer("Пришлите название профиля текстом")
        return
    label = message.text.strip()[:50]
    data = await state.get_data()
    telegram_id = message.from_user.id

    try:
        profile = await add_profile(
            telegram_id=telegram_id,
            label=label,
            login=data['login'],
            password=data['password'],
            institute=data['institute'],
        )
    except Exception as e:
        await message.answer(f"❌ Не удалось сохранить профиль: {e}")
        await state.clear()
        return

    await set_active_profile(telegram_id, profile.id)
    await state.clear()

    await message.answer(
        f"✅ Профиль «{label}» создан и сделан активным.\n\n"
        f"🔄 Запускаю первый парсинг (1–2 минуты)..."
    )

    result = await run_parse_for_profile(profile, telegram_id)
    await message.answer(result, reply_markup=main_menu_kb())


# ===== ДОБАВЛЕНИЕ ПРОФИЛЯ =====

@dp.callback_query(F.data == "profiles:add")
async def profiles_add_start(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    await callback.message.answer("Введите ЛОГИН Moodle для нового профиля:")
    await state.set_state(AddProfile.login)


@dp.message(AddProfile.login)
async def addp_login(message: types.Message, state: FSMContext):
    if not message.text:
        await message.answer("Пришлите логин текстом")
        return
    await state.update_data(login=message.text.strip())
    await message.answer("Введите ПАРОЛЬ:")
    await state.set_state(AddProfile.password)


@dp.message(AddProfile.password)
async def addp_password(message: types.Message, state: FSMContext):
    if not message.text:
        await message.answer("Пришлите пароль текстом")
        return
    await state.update_data(password=message.text.strip())
    await message.answer("Выберите ИНСТИТУТ:", reply_markup=institute_kb())
    await state.set_state(AddProfile.institute)


@dp.callback_query(AddProfile.institute, F.data.startswith("inst:"))
async def addp_institute(callback: CallbackQuery, state: FSMContext):
    institute = callback.data.split(":", 1)[1]
    await state.update_data(institute=institute)
    await callback.answer()
    await callback.message.edit_text(
        f"🏫 Институт: {institute}\n\nПридумайте название профиля:"
    )
    await state.set_state(AddProfile.label)


@dp.message(AddProfile.label)
async def addp_label(message: types.Message, state: FSMContext):
    if not message.text:
        await message.answer("Пришлите название профиля текстом")
        return
    label = message.text.strip()[:50]
    data = await state.get_data()
    try:
        profile = await add_profile(
            telegram_id=message.from_user.id,
            label=label,
            login=data['login'],
            password=data['password'],
            institute=data['institute'],
        )
    except Exception as e:
        await message.answer(f"❌ Не удалось: {e}")
        await state.clear()
        return
    await state.clear()
    await message.answer(
        f"✅ Профиль «{label}» добавлен.\n"
        f"Сделать активным можно в /profiles.",
        reply_markup=main_menu_kb(),
    )


# ===== МЕНЮ: ПАРСИНГ =====

@dp.callback_query(F.data == "menu:parse")
async def menu_parse(callback: CallbackQuery):
    await callback.answer()
    profiles = await list_profiles(callback.from_user.id)
    if not profiles:
        await callback.message.answer("У вас нет профилей. /start")
        return

    active = await get_active_profile(callback.from_user.id)
    buttons = []
    if active:
        buttons.append([InlineKeyboardButton(
            text=f"🔄 Активный ({active.label})", callback_data="parse:active"
        )])
    buttons.append([InlineKeyboardButton(text="🔄 Все профили", callback_data="parse:all")])
    await callback.message.answer(
        "Что обновить?", reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons)
    )


@dp.callback_query(F.data == "parse:active")
async def parse_active(callback: CallbackQuery):
    await callback.answer()
    profile = await get_active_profile(callback.from_user.id)
    if not profile:
        await callback.message.answer("Нет активного профиля.")
        return
    await callback.message.answer(f"🔄 Парсинг профиля «{profile.label}»...")
    result = await run_parse_for_profile(profile, callback.from_user.id)
    await callback.message.answer(result, reply_markup=main_menu_kb())


# >>> ИЗМЕНЕНО: parse_all собирает ошибки по профилям и сообщает о них
@dp.callback_query(F.data == "parse:all")
async def parse_all(callback: CallbackQuery):
    await callback.answer()
    profiles = await list_profiles(callback.from_user.id)
    if not profiles:
        await callback.message.answer("Нет профилей.")
        return
    await callback.message.answer(f"🔄 Парсинг {len(profiles)} профилей по очереди...")
    total = 0
    errors: list[str] = []
    for p in profiles:
        try:
            total += await _parse_one(p, callback.from_user.id)
        except MoodleAuthError as e:
            errors.append(f"• «{p.label}» ({p.institute}): {e}")
    text = f"✅ Готово. Обработано событий: {total}"
    if errors:
        text += "\n\n⚠️ Ошибки входа:\n" + "\n".join(errors)
    await callback.message.answer(text, reply_markup=main_menu_kb())
# <<< конец изменений


# >>> ИЗМЕНЕНО: _parse_one пробрасывает MoodleAuthError наверх
async def _parse_one(profile: MoodleProfile, telegram_id: int) -> int:
    url = INSTITUTE_URLS.get(profile.institute)
    if not url:
        raise MoodleAuthError(f"Неизвестный институт: {profile.institute}")
    try:
        count = await parse_moodle(profile, url)
        await add_parse_result(telegram_id, profile.institute, count)
        return count
    except MoodleAuthError:
        raise
    except Exception as e:
        log.exception("parse failed: %s", e)
        return 0


# >>> ИЗМЕНЕНО: run_parse_for_profile показывает причину ошибки
async def run_parse_for_profile(profile: MoodleProfile, telegram_id: int) -> str:
    try:
        count = await _parse_one(profile, telegram_id)
    except MoodleAuthError as e:
        return (
            f"❌ Не удалось войти в Moodle для профиля «{profile.label}».\n"
            f"Проверьте логин/пароль и выбранный институт ({profile.institute}).\n\n"
        )
    return f"✅ Парсинг завершён. Обработано событий: {count}"
# <<< конец изменений


# ===== МЕНЮ: СОБЫТИЯ =====

def _format_events(events: list[TestEvent], title: str, hidden: bool) -> tuple[str, InlineKeyboardMarkup]:
    if not events:
        return f"{title}\n\n📭 Пусто.", InlineKeyboardMarkup(inline_keyboard=[])

    text = f"{title}\n\n"
    buttons = []
    for i, ev in enumerate(events, 1):
        block = (
            f"{i}. <b>{ev.test_name or '(без названия)'}</b>\n"
            f"   📚 {ev.course_name or '—'}\n"
            f"   🔓 {ev.open_datetime or '—'}\n"
            f"   🔒 {ev.close_datetime or '—'}\n"
            f"   📌 {ev.status}\n"
            f"   🔗 {ev.url}\n\n"
        )
        if len(text) + len(block) > 3800:
            text += "…(обрезано)\n"
            break
        text += block
        if hidden:
            buttons.append([InlineKeyboardButton(
                text=f"👁 Вернуть #{i}", callback_data=f"unhide:{ev.id}"
            )])
        else:
            buttons.append([InlineKeyboardButton(
                text=f"🙈 Скрыть #{i}", callback_data=f"hide:{ev.id}"
            )])
    return text, InlineKeyboardMarkup(inline_keyboard=buttons)


@dp.callback_query(F.data == "menu:events")
async def menu_events(callback: CallbackQuery):
    await callback.answer()
    events = await list_events(callback.from_user.id, hidden=False, limit=20)
    text, kb = _format_events(events, "📅 Ваши события:", hidden=False)
    await callback.message.answer(text, reply_markup=kb, parse_mode="HTML",
                                  disable_web_page_preview=True)


@dp.callback_query(F.data == "menu:hidden")
async def menu_hidden(callback: CallbackQuery):
    await callback.answer()
    events = await list_events(callback.from_user.id, hidden=True, limit=20)
    text, kb = _format_events(events, "🙈 Скрытые события:", hidden=True)
    await callback.message.answer(text, reply_markup=kb, parse_mode="HTML",
                                  disable_web_page_preview=True)


@dp.callback_query(F.data.startswith("hide:"))
async def hide_event(callback: CallbackQuery):
    event_id = int(callback.data.split(":", 1)[1])
    await set_hidden(event_id, callback.from_user.id, True)
    await callback.answer("Скрыто")
    try:
        await callback.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass


@dp.callback_query(F.data.startswith("unhide:"))
async def unhide_event(callback: CallbackQuery):
    event_id = int(callback.data.split(":", 1)[1])
    await set_hidden(event_id, callback.from_user.id, False)
    await callback.answer("Возвращено")
    try:
        await callback.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass


# ===== МЕНЮ: ПРОФИЛИ =====

@dp.callback_query(F.data == "menu:profiles")
async def menu_profiles(callback: CallbackQuery):
    await callback.answer()
    profiles = await list_profiles(callback.from_user.id)
    active = await get_active_profile(callback.from_user.id)

    text = "👤 Ваши профили:\n\n"
    buttons = []
    if not profiles:
        text += "📭 Пока нет профилей."
    else:
        for p in profiles:
            mark = "✅ " if active and p.id == active.id else ""
            text += f"{mark}<b>{p.label}</b> — {p.login} ({p.institute})\n"
            row = []
            if not (active and p.id == active.id):
                row.append(InlineKeyboardButton(
                    text=f"✅ Сделать активным «{p.label}»", callback_data=f"prof:act:{p.id}"
                ))
            row.append(InlineKeyboardButton(
                text=f"🗑 Удалить «{p.label}»", callback_data=f"prof:del:{p.id}"
            ))
            buttons.append(row)
    buttons.append([InlineKeyboardButton(text="➕ Добавить профиль", callback_data="profiles:add")])
    await callback.message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
                                  parse_mode="HTML")


@dp.callback_query(F.data.startswith("prof:act:"))
async def prof_activate(callback: CallbackQuery):
    profile_id = int(callback.data.split(":", 2)[2])
    await set_active_profile(callback.from_user.id, profile_id)
    await callback.answer("Активный профиль обновлён")
    await callback.message.edit_text("✅ Активный профиль обновлён. /menu — в меню.")


@dp.callback_query(F.data.startswith("prof:del:"))
async def prof_delete(callback: CallbackQuery):
    profile_id = int(callback.data.split(":", 2)[2])
    profile = await get_profile(profile_id)
    if not profile or profile.telegram_id != callback.from_user.id:
        await callback.answer("Нет доступа", show_alert=True)
        return
    await delete_profile(profile_id)
    user = await get_user(callback.from_user.id)
    if user and user.active_profile_id == profile_id:
        await set_active_profile(callback.from_user.id, None)
    await callback.answer("Удалено")
    await callback.message.edit_text("🗑 Профиль и его события удалены. /menu — в меню.")


# ===== МЕНЮ: ИСТОРИЯ =====

@dp.callback_query(F.data == "menu:history")
async def menu_history(callback: CallbackQuery):
    await callback.answer()
    results = await list_parse_results(callback.from_user.id, limit=5)
    if not results:
        await callback.message.answer("📭 История пуста.")
        return
    text = "🕓 Последние парсинги:\n\n"
    for i, r in enumerate(results, 1):
        text += (
            f"{i}. 🏫 {r.institute}\n"
            f"   📊 Записей: {r.result_count}\n"
            f"   📅 {r.created_at.strftime('%d.%m.%Y %H:%M')}\n\n"
        )
    await callback.message.answer(text, reply_markup=main_menu_kb())


# ===== КОМАНДЫ-ФОЛБЭКИ =====

@dp.message(Command("events"))
async def cmd_events(message: types.Message):
    events = await list_events(message.from_user.id, hidden=False, limit=20)
    text, kb = _format_events(events, "📅 Ваши события:", hidden=False)
    await message.answer(text, reply_markup=kb, parse_mode="HTML",
                         disable_web_page_preview=True)


@dp.message(Command("hidden"))
async def cmd_hidden(message: types.Message):
    events = await list_events(message.from_user.id, hidden=True, limit=20)
    text, kb = _format_events(events, "🙈 Скрытые события:", hidden=True)
    await message.answer(text, reply_markup=kb, parse_mode="HTML",
                         disable_web_page_preview=True)


@dp.message(Command("profiles"))
async def cmd_profiles(message: types.Message):
    await cmd_menu(message)


@dp.message(Command("mydata"))
async def cmd_mydata(message: types.Message):
    profiles = await list_profiles(message.from_user.id)
    active = await get_active_profile(message.from_user.id)
    if not profiles:
        await message.answer("❌ Нет профилей. /start")
        return
    text = f"📋 Ваши данные (Telegram ID: {message.from_user.id}):\n\n"
    for p in profiles:
        mark = "✅ " if active and p.id == active.id else "• "
        text += f"{mark}<b>{p.label}</b> — {p.login} ({p.institute})\n"
    await message.answer(text, parse_mode="HTML")


@dp.message(Command("users"))
async def cmd_users(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("⛔ Нет прав.")
        return
    users = await list_all_users()
    if not users:
        await message.answer("📭 Нет пользователей.")
        return
    text = f"👥 Пользователи ({len(users)}):\n\n"
    for i, u in enumerate(users, 1):
        text += f"{i}. 🆔 {u.telegram_id} — {u.created_at.strftime('%d.%m.%Y')}\n"
        if len(text) > 3500:
            await message.answer(text)
            text = ""
    if text:
        await message.answer(text)


# ============================================================
#  ЗАПУСК
# ============================================================

async def main():
    await init_db()
    log.info("DB initialized")
    await set_commands()
    asyncio.create_task(cleanup_loop())
    log.info("Bot started")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
