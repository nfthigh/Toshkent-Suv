import logging
import asyncio
import aiosqlite
import re
import os
from datetime import datetime
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.enums import ParseMode, ChatType, ContentType
from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application
from aiohttp import web # Импорт для работы с веб-сервером

# --- Configuration values from environment variables ---
# IMPORTANT: These variables MUST be set in your Render environment!
API_TOKEN = os.environ.get('API_TOKEN')
ADMIN_CHAT_IDS_STR = os.environ.get('ADMIN_CHAT_IDS', '')
GROUP_CHAT_ID_STR = os.environ.get('GROUP_CHAT_ID')
PRICE_PER_BOTTLE_STR = os.environ.get('PRICE_PER_BOTTLE', '16000')

# !!! НОВЫЕ ПЕРЕМЕННЫЕ ДЛЯ WEBHOOK НА RENDER !!!
# Render предоставляет EXTERNAL_HOSTNAME (публичный адрес) и PORT (порт, который нужно слушать)
WEBHOOK_HOST = os.environ.get('RENDER_EXTERNAL_HOSTNAME') # Публичный hostname вашего сервиса Render
WEBAPP_PORT = os.environ.get('PORT') # Порт, который Render выделил и на котором нужно слушать

# Это секретный путь, по которому Telegram будет отправлять обновления.
# Установите эту переменную окружения на Render с длинным, случайным значением!
WEBHOOK_SECRET_PATH = os.environ.get('WEBHOOK_SECRET_PATH')

# Полный URL для Telegram Webhook (Render проксирует HTTPS на ваш порт)
WEBHOOK_URL = f"https://{WEBHOOK_HOST}{WEBHOOK_SECRET_PATH}"

# IP-адрес, который будет слушать ваш веб-сервер (0.0.0.0 означает все доступные интерфейсы)
WEBAPP_HOST = '0.0.0.0'

# Путь к файлу базы данных. Render сохраняет файлы в файловой системе сервиса.
# Указание имени файла или пути позволяет контролировать, где он будет создан/найден.
# На Render Free Tier это может быть не полностью персистентно между деплоями,
# но файл будет существовать пока жив контейнер или между "мягкими" перезапусками.
# Задание пути явно - хорошая практика.
DATABASE_PATH = os.environ.get('DATABASE_PATH', 'clients.db')


# Convert string variables to required types
ADMIN_CHAT_IDS = [int(chat_id.strip()) for chat_id in ADMIN_CHAT_IDS_STR.split(',') if chat_id.strip().isdigit()]
try:
    GROUP_CHAT_ID = int(GROUP_CHAT_ID_STR) if GROUP_CHAT_ID_STR else None
except (ValueError, TypeError):
     GROUP_CHAT_ID = None
     logging.warning("Environment variable GROUP_CHAT_ID is set incorrectly or missing. Group/channel notifications will not work.")

try:
    PRICE_PER_BOTTLE = int(PRICE_PER_BOTTLE_STR)
except (ValueError, TypeError):
    PRICE_PER_BOTTLE = 16000
    logging.warning(f"Environment variable PRICE_PER_BOTTLE is set incorrectly or missing. Using default value: {PRICE_PER_BOTTLE}")

# --- End configuration values ---

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Critical check for API token and Webhook essentials
if not API_TOKEN:
    logger.critical("Environment variable API_TOKEN is not set! The bot cannot be started.")
    exit(1)
if not WEBHOOK_HOST:
    logger.critical("Environment variable RENDER_EXTERNAL_HOSTNAME is not set! Webhook cannot be configured.")
    exit(1)
if not WEBAPP_PORT:
    logger.critical("Environment variable PORT is not set! Web server cannot be started.")
    exit(1)
if not WEBHOOK_SECRET_PATH:
    logger.critical("Environment variable WEBHOOK_SECRET_PATH is not set! Webhook URL is incomplete and insecure.")
    exit(1)


bot = Bot(token=API_TOKEN, timeout=60)
storage = MemoryStorage() # Use MemoryStorage for simplicity; consider FileStorage or RedisStorage for production state persistence
dp = Dispatcher(storage=storage)
db: aiosqlite.Connection = None # Global connection; initialized in main()


# --- Helper functions ---
def fmt_phone(num: str) -> str:
    """Formats a phone number, removing excess characters."""
    cleaned_num = re.sub(r'[^\d+]', '', num)
    # Optional: Add basic validation or re-formatting
    # Simplified check, adjust if needed for your locale
    if cleaned_num.startswith('998') and len(cleaned_num) == 12:
        return f"+{cleaned_num}"
    if len(cleaned_num) == 9 and cleaned_num.isdigit(): # Assume 901234567 -> +998901234567
         return f"+998{cleaned_num}"
    if cleaned_num.startswith('+998') and len(cleaned_num) == 13:
         return cleaned_num
    # Fallback for other formats or invalid input, keep as is
    return cleaned_num


def localize_date(dt: datetime, lang: str) -> str:
    """Localizes date and time in a given format."""
    day = dt.day
    year = dt.year
    time_str = dt.strftime("%H:%M")
    if lang == "ru":
        months = {1:"января", 2:"февраля", 3:"марта", 4:"апреля", 5:"мая", 6:"июня",
                  7:"июля", 8:"августа", 9:"сентября", 10:"октября", 11:"ноября", 12:"декабря"}
        return f"{day:02d} {months.get(dt.month, str(dt.month))} {year} г., {time_str}"
    else: # uz
        months = {1:"yanvar", 2:"fevral", 3:"mart", 4:"aprel", 5:"may", 6:"iyun",
                  7:"iyul", 8:"avgust", 9:"sentyabr", 10:"oktyabr", 11:"noyabr", 12:"dekabr"}
        return f"{day:02d} {months.get(dt.month, str(dt.month))} {year}, {time_str}"

async def get_user_lang(user_id: int, state: FSMContext = None) -> str:
    """
    Gets user language from FSM state, then from DB.
    If state is not provided or language not found anywhere, returns 'ru'.
    Also saves language to state if found in DB and state is provided.
    """
    # Try from state first
    if state:
        data = await state.get_data()
        if 'language' in data:
            return data['language']

    # If not in state or state not provided, try from DB
    if db:
        try:
            async with db.execute("SELECT language FROM clients WHERE user_id=?", (user_id,)) as cur:
                row = await cur.fetchone()
                if row and row[0]:
                    # If found in DB, save to state for faster future access
                    if state: await state.update_data(language=row[0])
                    return row[0]
        except Exception as e:
            logger.error(f"Error getting language from DB for user {user_id}: {e}")

    # If language not found in state or DB
    default_lang = 'ru'
    if state: await state.update_data(language=default_lang) # Save default to state
    return default_lang


async def is_user_registered(user_id: int) -> bool:
    """Checks if the user is registered (has a record with a non-empty name in DB)."""
    if db is None:
         logger.error("Database connection not established in is_user_registered.")
         return False
    try:
        async with db.execute("SELECT name FROM clients WHERE user_id=?", (user_id,)) as cur:
            row = await cur.fetchone()
            # User is considered registered if there's a record and the 'name' field is not empty
            return bool(row and row[0])
    except Exception as e:
        logger.error(f"Error checking user registration for {user_id}: {e}")
        return False # Assume not registered in case of error


# --- FSM States ---
class LangSelect(StatesGroup):
    choosing = State()

class OrderForm(StatesGroup):
    contact = State()
    name = State()
    location = State() # Can contain geolocation or flag for manual input
    address = State() # Text address, required after location
    additional = State()
    quantity = State()
    confirm = State()

class AdminStates(StatesGroup):
    main = State() # Main admin menu
    confirm_clear_clients = State()
    confirm_clear_orders = State()
    # Possibility to add state for order management if it becomes complex

# --- Localized Texts and Buttons ---
TEXT = {
    'ru': {
        'choose_language': "Выберите язык:",
        'welcome': "👋 Добро пожаловать, {name}!",
        'greeting_prompt': "👋 Добро пожаловать, {name}!\n\n", # Added separate text for greeting before the next step
        'send_contact': "Для начала, пожалуйста, отправьте ваш номер телефона.",
        'prompt_contact': "Пожалуйста, нажмите кнопку '📞 Отправить контакт' для отправки вашего номера.",
        'contact_saved': "✅ Контакт сохранён. Теперь введите ваше полное имя (имя и фамилия) или отправьте фото паспорта.",
        'please_full_name': "Пожалуйста, введите полное имя и фамилию текстом (например, 'Иван Иванов'), либо отправьте фото паспорта.",
        'name_saved': "Спасибо, {name}! Теперь отправьте локацию или введите адрес вручную.",
        'send_location': "Отправьте геолокацию или введите адрес вручную.",
        'address_prompt': "Укажите полный адрес доставки: район, улицу, номер дома и квартиры (если есть).",
        'additional_prompt': "Укажите дополнительный контактный номер (например, номер соседей или родственников) или нажмите 'Пропустить'.",
        'input_quantity': "Введите количество бутылей (шт.).\nЦена за бутылку: {price:,} сум.",
        'order_summary': "🛍️ Подтвердите ваш заказ:",
        'order_confirmed': "✅ Ваш заказ принят! Мы скоро свяжемся с вами для уточнения деталей.",
        'order_cancelled': "❌ Заказ отменён. Нажмите /start или '🔄 Начать сначала' для нового заказа.",
        'main_menu': "🏠 Главное меню:",
        'change_lang': "🔄 Сменить язык", # Language change button text
        'my_orders_title': "📦 Мои заказы:",
        'no_orders': "У вас пока нет заказов.",
        'order_info': "№{order_id} | {order_time} | {quantity} шт | Статус: {status}\nАдрес: {address}",
        'access_denied': "🚫 У вас нет доступа к этой команде.",
        'choose_admin_action': "🔧 Выберите действие с базой данных:",
        'clear_clients_confirm': "⚠️ Вы уверены, что хотите УДАЛИТЬ ВСЕХ клиентов И ИХ ЗАКАЗЫ? Это необратимо.",
        'clear_orders_confirm': "⚠️ Вы уверены, что хотите УДАЛИТЬ ВСЕ заказы? Это необратимо.",
        'db_clients_cleared': "✅ База данных клиентов (и заказов) очищена.",
        'db_orders_cleared': "✅ База данных заказов очищена.",
        'action_cancelled': "Действие отменено.",
        'feature_not_implemented': "🚧 Эта функция пока не реализована.",
        'invalid_input': "Неверный ввод. Пожалуйста, попробуйте еще раз или отмените процесс.",
        'back_to_main': "Возврат в главное меню.",
        'process_cancelled': "Процесс отменен.",
        'error_processing': "Произошла ошибка при обработке вашего запроса. Пожалуйста, попробуйте снова или свяжитесь с поддержкой.",
        'location_not_specified': 'Локация не указана',
        # Order statuses (keys should be consistent with DB)
        'status_pending': 'Ожидание обработки',
        'status_accepted': 'Принят',
        'status_in_progress': 'В работе',
        'status_completed': 'Выполнен',
        'status_rejected': 'Отменен',
        # Admin status buttons (inline) - text for buttons shown to ADMIN
        'admin_status_accept': '✅ Принять',
        'admin_status_reject': '❌ Отменить',
        'admin_status_complete': '📦 Выполнить',
        # Client notifications about status change
        'client_status_update': "📦 Статус вашего заказа №{order_id} обновлен: {status}\n\n{order_summary}",
        'admin_status_update_log': "Заказ №{order_id} переведен в статус '{status}' админом {admin_name} (@{admin_username}).",
        'order_already_finalized': "Статус заказа №{order_id} уже финальный ({status}). Изменение невозможно.",
        'order_not_found': "Заказ с ID {order_id} не найден."
    },
    'uz': {
        'choose_language': "Tilni tanlang:",
        'welcome': "👋 Xush kelibsiz, {name}!",
        'greeting_prompt': "👋 Xush kelibsiz, {name}!\n\n", # Added separate text
        'send_contact': "Boshlash uchun, iltimas, telefon raqamingizni yuboring.",
        'prompt_contact': "Iltimas, raqamingizni yuborish uchun '📞 Kontaktni yuborish' tugmasini bosing.",
        'contact_saved': "✅ Kontakt saqlandi. Endi to'liq ism va familiyangizni kiriting yoki pasport rasmini yuboring.",
        'please_full_name': "Iltimas, to'liq ism va familiyangizni matn shaklida kiriting (masalan, 'Ali Aliyev'), yoki pasport rasmini yuboring.",
        'name_saved': "Rahmat, {name}! Endi joylashuvingizni yuboring yoki manzilingizni qo'lda kiriting.",
        'send_location': "Geolokatsiyani yuboring yoki manzilni qo'lda kiriting.",
        'address_prompt': "To'liq yetkazib berish manzilini kiriting: tuman, ko'cha, uy va kvartira raqami (agar mavjud bo'lsa).",
        'additional_prompt': "Qo'shimcha aloqa raqamini kiriting (masalan, qo'shnilar yoki qarindoshlaringiz raqami) yoki 'O'tkazib yuborish' tugmasini bosing.",
        'input_quantity': "Iltimos, butilkalar sonini kiriting (dona).\nButilka narxi: {price:,} so'm.",
        'order_summary': "🛍️ Buyurtmangizni tasdiqlang:",
        'order_confirmed': "✅ Buyurtmangiz qabul qilindi! Tafsilotlarni aniqlash uchun tez orada siz bilan bog'lanamiz.",
        'order_cancelled': "❌ Buyurtma bekor qilindi. Yangi buyurtma berish uchun /start yoki '🔄 Yangi boshlash' tugmasini bosing.",
        'main_menu': "🏠 Bosh menyu:",
        'change_lang': "🔄 Tilni almashtirish", # Language change button text
        'my_orders_title': "📦 Mening buyurtmalarim:",
        'no_orders': "Sizda hali buyurtmalar yo'q.",
        'order_info': "№{order_id} | {order_time} | {quantity} dona | Holati: {status}\nManzil: {address}",
        'access_denied': "🚫 Bu buyruqqa ruxsat yo'q.",
        'choose_admin_action': "🔧 Ma'lumotlar bazasi bilan amalni tanlang:",
        'clear_clients_confirm': "⚠️ BARCHA mijozlarni VA ULARNING BUYURTMALARINI O'CHIRIB yubormoqchimisiz? Bu qaytarilmaydigan amal.",
        'clear_orders_confirm': "⚠️ BARCHA buyurtmalarni O'CHIRIB yubormoqchimisiz? Bu qaytarilmaydigan amal.",
        'db_clients_cleared': "✅ Mijozlar (va buyurtmalar) ma'lumotlar bazasi tozalandi.",
        'db_orders_cleared': "✅ Buyurtmalar ma'lumotlar bazasi tozalandi.",
        'action_cancelled': "Amal bekor qilindi.",
        'feature_not_implemented': "🚧 Bu funksiya hali ishga tushirilmagan.",
        'invalid_input': "Noto'g'ri kiritish. Iltimas, qaytadan urinib ko'ring yoki jarayonni bekor qiling.",
        'back_to_main': "Bosh menyuga qaytish.",
        'process_cancelled': "Jarayon bekor qilindi.",
        'error_processing': "So'rovingizni qayta ishlashda xatolik yuz berdi. Iltimas, qaytadan urinib ko'ring yoki qo'llab-quvvatlash xizmati bilan bog'laning.",
        'location_not_specified': 'Joylashuv belgilanmagan',
        # Order statuses (keys should be consistent with DB)
        'status_pending': 'Ishlov berish kutilmoqda',
        'status_accepted': 'Qabul qilindi',
        'status_in_progress': 'Jarayonda',
        'status_completed': 'Bajarildi',
        'status_rejected': 'Bekor qilindi',
        # Admin status buttons (inline) - text for buttons shown to ADMIN
        'admin_status_accept': '✅ Qabul qilish',
        'admin_status_reject': '❌ Bekor qilish',
        'admin_status_complete': '📦 Bajarildi',
        # Client notifications about status change
        'client_status_update': "📦 Sizning №{order_id} buyurtmangiz holati yangilandi: {status}\n\n{order_summary}",
        'admin_status_update_log': "Buyurtma №{order_id} holati admin {admin_name} (@{admin_username}) tomonidan '{status}' ga o'zgartirildi.",
        'order_already_finalized': "№{order_id} buyurtmasining holati allaqachon yakunlangan ({status}). O'zgartirish mumkin emas.",
        'order_not_found': "{order_id} ID raqamli buyurtma topilmadi."
    }
}

# Mapping status keys to texts
STATUS_MAP = {
    'pending': {'ru': TEXT['ru']['status_pending'], 'uz': TEXT['uz']['status_pending']},
    'accepted': {'ru': TEXT['ru']['status_accepted'], 'uz': TEXT['uz']['status_accepted']},
    'in_progress': {'ru': TEXT['ru']['status_in_progress'], 'uz': TEXT['uz']['status_in_progress']},
    'completed': {'ru': TEXT['ru']['status_completed'], 'uz': TEXT['uz']['status_completed']},
    'rejected': {'ru': TEXT['ru']['status_rejected'], 'uz': TEXT['uz']['status_rejected']},
}

# Mapping status keys for callbacks (to get 'accepted' from 'accept')
ADMIN_STATUS_CALLBACK_MAP = {
    'accept': 'accepted', # Accept -> Accepted
    'reject': 'rejected', # Reject -> Rejected
    'complete': 'completed', # Complete -> Completed
}


BTN = {
    'ru': {
        'send_contact': "📞 Отправить контакт",
        'cancel': "❌ Отменить",
        'send_location': "📍 Отправить локацию",
        'enter_address': "🏠 Ввести адрес вручную",
        'start_over': "🔄 Начать сначала",
        'my_orders': "📦 Мои заказы",
        'edit_order': "✏️ Редактировать заказ", # Not implemented yet
        'manage_db': "🔧 Управление базой данных", # Admin only
        'skip': "Пропустить",
        'back': "⬅️ Назад",
        # Admin buttons (inline) - text for buttons shown to ADMIN (use TEXT dict)
        'admin_clear_clients': "🗑️ Очистить клиентов",
        'admin_clear_orders': "🗑️ Очистить заказы",
        'admin_confirm_yes': "✅ Да",
        'admin_confirm_no': "❌ Нет",
    },
    'uz': {
        'send_contact': "📞 Kontaktni yuborish",
        'cancel': "❌ Bekor qilish",
        'send_location': "📍 Joylashuvni yuboring",
        'enter_address': "🏠 Manzilni qo'lda kiritish",
        'start_over': "🔄 Yangi boshlash",
        'my_orders': "📦 Buyurtmalarim",
        'edit_order': "✏️ Buyurtmani tahrirlash", # Not implemented yet
        'manage_db': "🔧 Bazani boshqarish", # Admin only
        'skip': "O'tkazib yuborish",
        'back': "⬅️ Orqaga",
        # Admin buttons (inline) - text for buttons shown to ADMIN (use TEXT dict)
        'admin_clear_clients': "🗑️ Mijozlarni tozalash",
        'admin_clear_orders': "🗑️ Buyurtmalarni tozalash",
        'admin_confirm_yes': "✅ Ha",
        'admin_confirm_no': "❌ Yo'q",
    }
}


# --- Functions for creating keyboards ---
def kb_main(lang, is_admin=False, is_registered=False):
    """Main menu keyboard"""
    kb = []
    # Only show "Send Contact" if the user is not registered (name is missing)
    if not is_registered:
        kb.append([KeyboardButton(text=BTN[lang]['send_contact'], request_contact=True)])

    kb.append([KeyboardButton(text=BTN[lang]['my_orders'])])

    # "Edit Order" button not implemented yet
    # if is_registered:
    #    kb.append([KeyboardButton(text=BTN[lang]['edit_order'])])

    kb.append([KeyboardButton(text=BTN[lang]['start_over'])])

    if is_admin:
        kb.append([KeyboardButton(text=BTN[lang]['manage_db'])])

    kb.append([KeyboardButton(text=TEXT[lang]['change_lang'])]) # Language change button

    return ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)

def kb_location(lang):
    """Keyboard for location selection/manual address input"""
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=BTN[lang]['send_location'], request_location=True)],
            [KeyboardButton(text=BTN[lang]['enter_address'])],
            [KeyboardButton(text=BTN[lang]['cancel'])]
        ],
        resize_keyboard=True
    )

def kb_cancel_back(lang):
    """Keyboard with Cancel and Back buttons"""
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=BTN[lang]['back'])],
            [KeyboardButton(text=BTN[lang]['cancel'])]
        ],
        resize_keyboard=True
    )

def kb_additional(lang):
    """Keyboard for additional contact with Skip, Back, Cancel"""
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=BTN[lang]['skip'])],
            [KeyboardButton(text=BTN[lang]['back'])],
            [KeyboardButton(text=BTN[lang]['cancel'])]
        ],
        resize_keyboard=True
    )

def kb_quantity(lang):
    """Keyboard for quantity input with Back, Cancel"""
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=BTN[lang]['back'])],
            [KeyboardButton(text=BTN[lang]['cancel'])]
        ],
        resize_keyboard=True
    )

def kb_language_select():
    """Language selection keyboard"""
    # Language selection keyboard text should probably be static or detect browser lang,
    # but for simplicity, we use hardcoded buttons.
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text='🇷🇺 Русский'), KeyboardButton(text='🇺🇿 Ўзбек')]],
        resize_keyboard=True
    )

def kb_admin_db(lang):
    """Inline keyboard for admin DB actions"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=BTN[lang]['admin_clear_clients'], callback_data="admin_clear_clients")],
        [InlineKeyboardButton(text=BTN[lang]['admin_clear_orders'], callback_data="admin_clear_orders")],
    ])

def kb_admin_confirm(lang, action_type):
    """Inline confirmation keyboard for admin action"""
    # action_type will be either 'clients' or 'orders'
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text=BTN[lang]['admin_confirm_yes'], callback_data=f"admin_confirm_{action_type}_yes"),
            InlineKeyboardButton(text=BTN[lang]['admin_confirm_no'], callback_data=f"admin_confirm_{action_type}_no")
        ]
    ])

def kb_admin_order_status(order_id: int, lang: str) -> InlineKeyboardMarkup:
    """Inline keyboard for changing order status by admins."""
    # Buttons should send callback_data in format "set_status:<order_id>:<status_key>"
    # Admin buttons texts usually on one language (e.g., Russian) for admin chat convenience.
    # Here, we use the admin's language from TEXT[lang].
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text=TEXT[lang]['admin_status_accept'], callback_data=f"set_status:{order_id}:accept"),
            InlineKeyboardButton(text=TEXT[lang]['admin_status_reject'], callback_data=f"set_status:{order_id}:reject")
        ],
        [
            InlineKeyboardButton(text=TEXT[lang]['admin_status_complete'], callback_data=f"set_status:{order_id}:complete")
        ]
    ])
    return kb


# --- Handlers for general buttons (work regardless of state or in specific states) ---

# Handler for "Cancel" button (works in any OrderForm state)
@dp.message(StateFilter(OrderForm), F.text.in_([BTN['ru']['cancel'], BTN['uz']['cancel']]))
async def handle_cancel_btn(message: types.Message, state: FSMContext):
    await cancel_process(message, state)

# Handler for "Back" button (works in specific OrderForm states)
@dp.message(StateFilter(OrderForm.address, OrderForm.additional, OrderForm.quantity), F.text.in_([BTN['ru']['back'], BTN['uz']['back']]))
async def handle_back_btn(message: types.Message, state: FSMContext):
    data = await state.get_data()
    lang = await get_user_lang(message.from_user.id, state)

    current_state = await state.get_state()

    if current_state == OrderForm.address.state:
        # From address back to location
        await message.reply(TEXT[lang]['send_location'], reply_markup=kb_location(lang))
        await state.set_state(OrderForm.location)
        await state.update_data(address=None) # Reset entered address
    elif current_state == OrderForm.additional.state:
        # From additional back to address
        await message.reply(TEXT[lang]['address_prompt'], reply_markup=kb_cancel_back(lang))
        await state.set_state(OrderForm.address)
        await state.update_data(additional_contact=None) # Reset additional contact
    elif current_state == OrderForm.quantity.state:
        # From quantity back to additional
        await message.reply(TEXT[lang]['additional_prompt'], reply_markup=kb_additional(lang))
        await state.set_state(OrderForm.additional)
        await state.update_data(quantity=None) # Reset quantity

# Handler for "Skip" button (works in OrderForm.additional state)
@dp.message(OrderForm.additional, F.text.in_([BTN['ru']['skip'], BTN['uz']['skip']]))
async def handle_skip_btn(message: types.Message, state: FSMContext):
    data = await state.get_data()
    lang = await get_user_lang(message.from_user.id, state)
    await state.update_data(additional_contact=None) # Save as None
    await message.reply(TEXT[lang]['input_quantity'].format(price=PRICE_PER_BOTTLE), reply_markup=kb_quantity(lang))
    await state.set_state(OrderForm.quantity)

# Handler for "Start Over" button (works in any state)
@dp.message(F.text.in_([BTN['ru']['start_over'], BTN['uz']['start_over']]))
async def handle_start_over_btn(message: types.Message, state: FSMContext):
    await cmd_start(message, state) # Essentially restarts the process like /start

# Handler for "Change Language" button (works in any state)
@dp.message(F.text.in_([TEXT['ru']['change_lang'], TEXT['uz']['change_lang']]))
async def handle_change_lang_btn(message: types.Message, state: FSMContext):
    await state.clear() # Clear current state (including order)
    await message.reply(TEXT['ru']['choose_language'], reply_markup=kb_language_select())
    await state.set_state(LangSelect.choosing)

# Handler for "My Orders" button (works in any state)
@dp.message(F.text.in_([BTN['ru']['my_orders'], BTN['uz']['my_orders']]))
async def handle_my_orders_btn(message: types.Message, state: FSMContext):
    uid = message.from_user.id
    lang = await get_user_lang(uid, state)

    is_registered = await is_user_registered(uid)

    # Allow checking orders only for registered users
    if not is_registered:
         await message.reply(TEXT[lang]['access_denied'], reply_markup=kb_main(lang, uid in ADMIN_CHAT_IDS, is_registered))
         await state.clear()
         return

    try:
        async with db.execute("SELECT order_id, order_time, quantity, status, address, location_lat, location_lon FROM orders WHERE user_id=? ORDER BY order_time DESC", (uid,)) as cur:
            orders = await cur.fetchall()
    except Exception as e:
        logger.error(f"Error getting orders for user {uid}: {e}")
        await message.reply(TEXT[lang]['error_processing'], reply_markup=kb_main(lang, uid in ADMIN_CHAT_IDS, True))
        await state.clear()
        return


    if not orders:
        await message.reply(TEXT[lang]['no_orders'], reply_markup=kb_main(lang, uid in ADMIN_CHAT_IDS, True))
        await state.clear()
        return

    order_list = [TEXT[lang]['my_orders_title']]
    for order in orders:
        order_id, order_time_str, quantity, status_key, address, lat, lon = order
        try:
            order_dt = datetime.strptime(order_time_str, "%Y-%m-%d %H:%M:%S")
            localized_order_time = localize_date(order_dt, lang)
        except (ValueError, TypeError):
            logger.warning(f"Date parsing error for order {order_id}: {order_time_str}")
            localized_order_time = order_time_str # Use raw string if parsing fails

        # Get localized status text
        localized_status = STATUS_MAP.get(status_key, {}).get(lang, status_key)

        # Determine how to show address/location
        display_address = address if address else (TEXT[lang]['location_not_specified'] if lat is None else TEXT[lang].get('location', 'Location/Joylashuv'))


        order_list.append(
            TEXT[lang]['order_info'].format(
                order_id=order_id,
                order_time=localized_order_time,
                quantity=quantity,
                status=localized_status,
                address=display_address
            )
        )

    await message.reply("\n\n".join(order_list), reply_markup=kb_main(lang, uid in ADMIN_CHAT_IDS, True))
    await state.clear()

# Handler for "Edit Order" button (placeholder)
@dp.message(F.text.in_([BTN['ru']['edit_order'], BTN['uz']['edit_order']]))
async def handle_edit_order_btn(message: types.Message, state: FSMContext):
    uid = message.from_user.id
    lang = await get_user_lang(uid, state)

    is_registered = await is_user_registered(uid)

    if is_registered:
        await message.reply(TEXT[lang]['feature_not_implemented'], reply_markup=kb_main(lang, uid in ADMIN_CHAT_IDS, True))
        await state.clear()
    else:
        # This button shouldn't typically appear for unregistered users, but handle defensively
        await message.reply(TEXT[lang]['access_denied'], reply_markup=kb_main(lang, uid in ADMIN_CHAT_IDS, False))
        await state.clear()


# --- Admin button handlers ---

# Handler for "Manage Database" button
@dp.message(F.text.in_([BTN['ru']['manage_db'], BTN['uz']['manage_db']]))
async def handle_manage_db_btn(message: types.Message, state: FSMContext):
    uid = message.from_user.id
    lang = await get_user_lang(uid, state) # Use admin's language

    if uid not in ADMIN_CHAT_IDS:
        await message.reply(TEXT[lang]['access_denied'], reply_markup=kb_main(lang, False, await is_user_registered(uid)))
        await state.clear()
        return

    await message.reply(TEXT[lang]['choose_admin_action'], reply_markup=kb_admin_db(lang))
    await state.set_state(AdminStates.main)


# Handlers for inline admin actions (clear)
@dp.callback_query(AdminStates.main, F.data.startswith("admin_clear_"))
async def handle_admin_clear_callback(callback: types.CallbackQuery, state: FSMContext):
    await callback.answer()
    uid = callback.from_user.id
    lang = await get_user_lang(uid, state) # Use admin's language

    action = callback.data.split('_')[-1]

    if action == 'clients':
        confirm_text = TEXT[lang]['clear_clients_confirm']
        confirm_kb = kb_admin_confirm(lang, 'clients')
        await state.set_state(AdminStates.confirm_clear_clients)
    elif action == 'orders':
        confirm_text = TEXT[lang]['clear_orders_confirm']
        confirm_kb = kb_admin_confirm(lang, 'orders')
        await state.set_state(AdminStates.confirm_clear_orders)
    else:
        # Should not happen with correct callback data
        await callback.message.edit_text(TEXT[lang]['invalid_input'], reply_markup=None)
        await state.clear()
        return

    await callback.message.edit_text(confirm_text, reply_markup=confirm_kb)


# Handler for confirming client clear
@dp.callback_query(AdminStates.confirm_clear_clients, F.data.startswith("admin_confirm_clients_"))
async def handle_confirm_clear_clients(callback: types.CallbackQuery, state: FSMContext):
    await callback.answer()
    uid = callback.from_user.id
    lang = await get_user_lang(uid, state) # Use admin's language

    action = callback.data.split('_')[-1]

    if action == 'yes':
        try:
            # DELETE FROM clients with CASCADE will also delete associated orders
            await db.execute("DELETE FROM clients")
            await db.commit()
            await callback.message.edit_text(TEXT[lang]['db_clients_cleared'], reply_markup=None)
            logger.info(f"Admin {uid} cleared clients (and orders) database.")
        except Exception as e:
            logger.error(f"Error clearing clients/orders (admin {uid}): {e}")
            await callback.message.edit_text(f"{TEXT[lang]['error_processing']} Error: {e}", reply_markup=None)
    else: # action == 'no'
        await callback.message.edit_text(TEXT[lang]['action_cancelled'], reply_markup=None)
        logger.info(f"Admin {uid} cancelled client clear.")

    await state.clear()
    is_registered = await is_user_registered(uid)
    # Send main menu after admin action
    await bot.send_message(uid, TEXT[lang]['back_to_main'], reply_markup=kb_main(lang, uid in ADMIN_CHAT_IDS, is_registered))


# Handler for confirming order clear
@dp.callback_query(AdminStates.confirm_clear_orders, F.data.startswith("admin_confirm_orders_"))
async def handle_confirm_clear_orders(callback: types.CallbackQuery, state: FSMContext):
    await callback.answer()
    uid = callback.from_user.id
    lang = await get_user_lang(uid, state) # Use admin's language

    action = callback.data.split('_')[-1]

    if action == 'yes':
        try:
            await db.execute("DELETE FROM orders")
            await db.commit()
            await callback.message.edit_text(TEXT[lang]['db_orders_cleared'], reply_markup=None)
            logger.info(f"Admin {uid} cleared orders database.")
        except Exception as e:
            logger.error(f"Error clearing orders (admin {uid}): {e}")
            await callback.message.edit_text(f"{TEXT[lang]['error_processing']} Error: {e}", reply_markup=None)
    else: # action == 'no'
        await callback.message.edit_text(TEXT[lang]['action_cancelled'], reply_markup=None)
        logger.info(f"Admin {uid} cancelled order clear.")

    await state.clear()
    is_registered = await is_user_registered(uid)
    # Send main menu after admin action
    await bot.send_message(uid, TEXT[lang]['back_to_main'], reply_markup=kb_main(lang, uid in ADMIN_CHAT_IDS, is_registered))


# --- Handler for admin order status change ---
@dp.callback_query(F.data.startswith("set_status:"))
async def handle_admin_set_status(callback: types.CallbackQuery): # No state needed for this handler
    # Answer callback immediately
    await callback.answer()

    uid = callback.from_user.id
    # Determine admin language. We can try to get it from DB directly
    # as state might not be active for admin actions.
    admin_lang = 'ru' # Default admin lang
    try:
         async with db.execute("SELECT language FROM clients WHERE user_id=?", (uid,)) as cur:
              row = await cur.fetchone()
              if row and row[0]:
                   admin_lang = row[0]
    except Exception as e:
         logger.warning(f"Could not retrieve admin language for {uid}: {e}")


    admin_name = callback.from_user.full_name
    admin_username = callback.from_user.username or "N/A"


    if uid not in ADMIN_CHAT_IDS:
        await callback.answer(TEXT[admin_lang]['access_denied'], show_alert=True)
        return

    try:
        # Parse callback_data: set_status:<order_id>:<status_key>
        parts = callback.data.split(':')
        if len(parts) != 3:
            await callback.answer(TEXT[admin_lang]['invalid_input'], show_alert=True)
            return
        order_id = int(parts[1])
        action_key = parts[2] # 'accept', 'reject', 'complete'
        new_status_key = ADMIN_STATUS_CALLBACK_MAP.get(action_key)

        if not new_status_key:
            await callback.answer(TEXT[admin_lang]['invalid_input'], show_alert=True)
            return

        # Get current status, client_id, and all order data for summary
        async with db.execute("SELECT user_id, status, contact, additional_contact, address, quantity, order_time, location_lat, location_lon FROM orders WHERE order_id=?", (order_id,)) as cur:
            order_row = await cur.fetchone()

        if not order_row:
            await callback.answer(TEXT[admin_lang]['order_not_found'].format(order_id=order_id), show_alert=True)
            try:
                await callback.message.edit_reply_markup(reply_markup=None) # Remove buttons if order not found
            except Exception as e:
                logger.warning(f"Failed to remove buttons from order message {order_id}: {e}")
            return

        client_id, current_status_key, contact, additional_contact, address, quantity, order_time_str, lat, lon = order_row

        # Check if the current status is final
        final_statuses = ['completed', 'rejected'] # Keys of final statuses
        if current_status_key in final_statuses:
            await callback.answer(TEXT[admin_lang]['order_already_finalized'].format(order_id=order_id, status=STATUS_MAP.get(current_status_key,{}).get(admin_lang, current_status_key)), show_alert=True)
            # Remove buttons if status is final
            try:
                await callback.message.edit_reply_markup(reply_markup=None)
            except Exception as e:
                 logger.warning(f"Failed to remove buttons from finalized order {order_id} in chat {callback.message.chat.id}: {e}")
            return

        # Update status in DB
        await db.execute("UPDATE orders SET status=? WHERE order_id=?", (new_status_key, order_id))
        await db.commit()
        logger.info(f"Order №{order_id} status updated to '{new_status_key}' by admin {uid}")

        # Get localized text for the new status for the admin
        new_status_text_admin = STATUS_MAP.get(new_status_key, {}).get(admin_lang, new_status_key)


        # Edit the message in the admin chat/group
        try:
            # Add info about the admin who changed the status
            log_message = TEXT[admin_lang]['admin_status_update_log'].format(
                order_id=order_id,
                status=new_status_text_admin, # Use localized text for admin
                admin_name=admin_name,
                admin_username=admin_username
            )
            # Update the message text, adding the log and new status
            current_text = callback.message.text
            # Find the existing status line using the pattern "✨ Статус: ..."
            status_line_pattern = re.compile(r"✨ Статус: .*")
            match = status_line_pattern.search(current_text)

            updated_text = current_text # Start with the original text

            if match:
                 # Replace the existing status line
                 updated_text = status_line_pattern.sub(f"✨ Статус: {new_status_text_admin}", updated_text)
            else:
                 # If status line wasn't found, append it (fallback)
                 updated_text += f"\n✨ Статус: {new_status_text_admin}"

            # Ensure the log message is appended, handling previous logs if any
            log_start_index = updated_text.find("<i>")
            if log_start_index != -1:
                 # Replace existing logs if present
                 updated_text = updated_text[:log_start_index].strip() + f"\n\n<i>{log_message}</i>"
            else:
                 # Add new log if no previous logs
                 updated_text += f"\n\n<i>{log_message}</i>"


            await callback.message.edit_text(
                updated_text,
                parse_mode=ParseMode.HTML,
                reply_markup=None # Remove buttons after processing the change
            )
        except Exception as e:
            logger.error(f"Failed to edit order message {order_id} in chat {callback.message.chat.id}: {e}")
            # If editing fails, send a new log message
            try:
                 retry_log_message = TEXT[admin_lang]['admin_status_update_log'].format(
                    order_id=order_id,
                    status=new_status_text_admin,
                    admin_name=admin_name,
                    admin_username=admin_username
                 )
                 await bot.send_message(callback.message.chat.id, f"<i>{retry_log_message}</i>", parse_mode=ParseMode.HTML)
                 # Also try to remove buttons from the original message if editing text failed but markup might succeed
                 try:
                      await callback.message.edit_reply_markup(reply_markup=None)
                 except Exception as e2:
                      logger.warning(f"Failed to remove buttons from order message {order_id} after edit failure: {e2}")

            except Exception as e3:
                 logger.error(f"Failed to send log message about status update for order {order_id}: {e3}")


        # Notify the client about the status change
        client_lang = await get_user_lang(client_id) # Get client's language
        client_new_status_text = STATUS_MAP.get(new_status_key, {}).get(client_lang, new_status_key) # Localize status for client

        # Formulate order summary for the client
        total = quantity * PRICE_PER_BOTTLE
        display_address = address if address else (TEXT[client_lang]['location_not_specified'] if lat is None else TEXT[client_lang].get('location', 'Location/Joylashuv'))

        # Get client name and contact for the summary
        client_info_db = {}
        try:
            async with db.execute("SELECT name, contact, username FROM clients WHERE user_id=?", (client_id,)) as cur:
                row = await cur.fetchone()
                if row:
                    client_info_db = {"name": row[0], "contact": row[1], "username": row[2]}
        except Exception as e:
             logger.error(f"Error fetching client info {client_id} for status notification: {e}")
             # Fallback to data from order_row if DB fetch fails
             client_info_db = {"name": "N/A", "contact": contact, "username": ""} # Use contact from order if name/username missing


        client_summary = (
            f"👤 {client_info_db.get('name', 'N/A')}" + (f" (@{client_info_db.get('username')})" if client_info_db.get('username') else "") + "\n"
            f"📞 Основной: {client_info_db.get('contact')}\n" # Use contact from DB first, then order_row fallback (less reliable)
            f"📞 Доп.: {additional_contact or ('–' if client_lang == 'ru' else '–')}\n"
            f"📍 Адрес: {display_address}\n"
            f"🔢 Количество: {quantity} " + ("шт" if client_lang == "ru" else "dona") + f" (Общая сумма: {total:,} " + ("сум" if client_lang == "ru" else "so'm") + ")\n"
        )

        client_notification_text = TEXT[client_lang]['client_status_update'].format(
            order_id=order_id,
            status=client_new_status_text, # Use localized text for the client
            order_summary=client_summary
        )
        try:
            await bot.send_message(client_id, client_notification_text)
        except Exception as e:
            # Client might have blocked the bot
            logger.error(f"Failed to send status update notification for order {order_id} to client {client_id}: {e}")


    except Exception as e:
        logger.error(f"Error processing status change callback {callback.data} by admin {uid}: {e}")
        await callback.answer(TEXT[admin_lang]['error_processing'], show_alert=True)


# --- Main handlers for order process ---

@dp.message(Command("start"))
async def cmd_start(message: types.Message, state: FSMContext):
    logger.info(f"/start received from {message.from_user.id} in chat: {message.chat.type}")
    # Check if it's a private chat. Bot only works in private chats for users.
    if message.chat.type != ChatType.PRIVATE:
         # Optionally send a message to the group/channel saying "I only work in private chats"
         # await message.reply("I only work in private chats. Please contact me directly.")
         return # Ignore commands in groups/channels

    await state.clear() # Always clear state on /start to begin a new process
    uid = message.from_user.id

    is_registered = False
    lang = 'ru' # Default language
    name = None

    try:
        async with db.execute("SELECT name, language FROM clients WHERE user_id=?", (uid,)) as cur:
            row = await cur.fetchone()
            if row:
                db_name, db_lang = row
                # If user exists in DB, use their saved language
                lang = db_lang or 'ru'
                await state.update_data(language=lang) # Save language to state

                if db_name: # If name is filled - user is registered
                    name = db_name
                    await state.update_data(name=name) # Save name to state
                    is_registered = True
                    logger.info(f"User {uid} is registered. Language: {lang}. Name: {name}") # Log name
                else: # User selected language but didn't finish name registration
                     logger.info(f"User {uid} did not finish registration. Language: {lang}")
                     # Prompt to send contact
                     await state.set_state(OrderForm.contact)
                     await message.reply(TEXT[lang]['send_contact'],
                                         reply_markup=ReplyKeyboardMarkup(
                                             keyboard=[[KeyboardButton(text=BTN[lang]['send_contact'], request_contact=True)],
                                                       [KeyboardButton(text=BTN[lang]['cancel'])]],
                                             resize_keyboard=True))
                     return # Exit handler


    except Exception as e:
        logger.error(f"Error in cmd_start checking user {uid}: {e}")
        # In case of DB error or other issue, fallback to asking for language
        await message.reply(TEXT['ru']['error_processing'], reply_markup=kb_language_select()) # Use RU for initial error message
        await state.set_state(LangSelect.choosing)
        return

    # If user is registered (is_registered == True)
    if is_registered:
        # Send greeting and prompt to start order
        greeting_text = TEXT[lang]['greeting_prompt'].format(name=name)
        next_step_text = TEXT[lang]['send_location']
        await message.reply(greeting_text + next_step_text, reply_markup=kb_location(lang))
        await state.set_state(OrderForm.location)
    else:
        # User is new (row is None) or exists but name is empty
        logger.info(f"User {uid} is new or unregistered name. Prompting language selection.")
        await message.reply(TEXT['ru']['choose_language'], reply_markup=kb_language_select())
        await state.set_state(LangSelect.choosing)


@dp.message(LangSelect.choosing, F.text.in_(["🇷🇺 Русский", "🇺🇿 Ўзбек"]))
async def process_lang(message: types.Message, state: FSMContext):
    lang = "ru" if message.text.startswith("🇷🇺") else "uz"
    await state.update_data(language=lang)
    uid = message.from_user.id
    usernm = message.from_user.username or ""

    try:
        # Insert or update client record with language and username
        await db.execute(
            "INSERT INTO clients(user_id, username, language) VALUES(?, ?, ?) "
            "ON CONFLICT(user_id) DO UPDATE SET username=excluded.username, language=excluded.language",
            (uid, usernm, lang)
        )
        await db.commit()
        logger.info(f"User {uid} selected language: {lang}")
    except Exception as e:
         logger.error(f"Error in process_lang saving client {uid}: {e}")
         await message.reply(TEXT[lang]['error_processing'])
         await state.clear()
         # is_user_registered will check name presence, which is still None/empty here
         await message.reply(TEXT[lang]['process_cancelled'], reply_markup=kb_main(lang, uid in ADMIN_CHAT_IDS, False)) # Use False as registration is not complete
         return

    await message.reply(TEXT[lang]['send_contact'],
                        reply_markup=ReplyKeyboardMarkup(
                            keyboard=[[KeyboardButton(text=BTN[lang]['send_contact'], request_contact=True)],
                                      [KeyboardButton(text=BTN[lang]['cancel'])]],
                            resize_keyboard=True))
    await state.set_state(OrderForm.contact)


@dp.message(OrderForm.contact, F.content_type == "contact")
async def reg_contact(message: types.Message, state: FSMContext):
    data = await state.get_data()
    lang = await get_user_lang(message.from_user.id, state)
    num = message.contact.phone_number
    formatted = fmt_phone(num)
    uid = message.from_user.id
    usernm = message.from_user.username or ""

    try:
        # Update client record with contact and username
        await db.execute(
            "UPDATE clients SET contact=?, username=? WHERE user_id=?",
            (formatted, usernm, uid))
        await db.commit()
        logger.info(f"User {uid} saved contact: {formatted}")
    except Exception as e:
         logger.error(f"Error in reg_contact updating client {uid}: {e}")
         await message.reply(TEXT[lang]['error_processing'])
         await state.clear()
         # Check registration status again after error
         await message.reply(TEXT[lang]['process_cancelled'], reply_markup=kb_main(lang, uid in ADMIN_CHAT_IDS, await is_user_registered(uid)))
         return

    await state.update_data(contact=formatted)
    # Prompt for name/passport photo
    await message.reply(TEXT[lang]['contact_saved'],
                        reply_markup=ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text=BTN[lang]['cancel'])]], resize_keyboard=True))
    await state.set_state(OrderForm.name)

@dp.message(OrderForm.contact) # Catches any other text/content type in this state
async def prompt_contact_again(message: types.Message, state: FSMContext):
    data = await state.get_data()
    lang = await get_user_lang(message.from_user.id, state)
    # "Cancel" button is handled by handle_cancel_btn separately

    # Reply with the contact prompt again
    await message.reply(TEXT[lang]['prompt_contact'],
                        reply_markup=ReplyKeyboardMarkup(keyboard=[
                            [KeyboardButton(text=BTN[lang]['send_contact'], request_contact=True)],
                            [KeyboardButton(text=BTN[lang]['cancel'])]
                        ], resize_keyboard=True))

@dp.message(OrderForm.name, F.content_type == "text")
async def reg_name_text(message: types.Message, state: FSMContext):
    data = await state.get_data()
    lang = await get_user_lang(message.from_user.id, state)
    name = message.text.strip()

    # "Cancel" button is handled by handle_cancel_btn separately
    # This handler will only process text that is NOT the Cancel button.

    # Basic validation: check if it looks like first and last name
    if len(name.split()) < 2:
         return await message.reply(TEXT[lang]['please_full_name'])

    uid = message.from_user.id
    try:
        # Update client record with name
        await db.execute("UPDATE clients SET name=? WHERE user_id=?", (name, uid))
        await db.commit()
        logger.info(f"User {uid} saved name (text): {name}")
    except Exception as e:
         logger.error(f"Error in reg_name_text updating client {uid}: {e}")
         await message.reply(TEXT[lang]['error_processing'])
         await state.clear()
         await message.reply(TEXT[lang]['process_cancelled'], reply_markup=kb_main(lang, uid in ADMIN_CHAT_IDS, await is_user_registered(uid)))
         return

    await state.update_data(name=name) # Save name to state
    # Prompt for location/address
    await message.reply(TEXT[lang]['name_saved'].format(name=name), reply_markup=kb_location(lang))
    await state.set_state(OrderForm.location)

@dp.message(OrderForm.name, F.content_type == "photo")
async def reg_name_photo(message: types.Message, state: FSMContext):
    data = await state.get_data()
    lang = await get_user_lang(message.from_user.id, state)
    file_id = message.photo[-1].file_id
    uid = message.from_user.id
    name = f"Passport photo: {file_id}" # Store passport info as name

    try:
        # Update client record with name (indicating photo was sent)
        await db.execute("UPDATE clients SET name=? WHERE user_id=?", (name, uid))
        await db.commit()
        logger.info(f"User {uid} saved passport photo file_id: {file_id}")
    except Exception as e:
         logger.error(f"Error in reg_name_photo updating client {uid}: {e}")
         await message.reply(TEXT[lang]['error_processing'])
         await state.clear()
         await message.reply(TEXT[lang]['process_cancelled'], reply_markup=kb_main(lang, uid in ADMIN_CHAT_IDS, await is_user_registered(uid)))
         return

    await state.update_data(name=name) # Save photo info as name in state
    # Prompt for location/address
    await message.reply(TEXT[lang]['name_saved'].format(name=TEXT[lang].get('by_photo', 'по фото')), reply_markup=kb_location(lang)) # Show "by photo" placeholder
    await state.set_state(OrderForm.location)

@dp.message(OrderForm.location, F.content_type == "location")
async def loc_received(message: types.Message, state: FSMContext):
    data = await state.get_data()
    lang = await get_user_lang(message.from_user.id, state)
    loc = message.location
    await state.update_data(location_lat=loc.latitude, location_lon=loc.longitude, address=None) # Reset address if location is sent
    # Prompt for address clarification (optional but good practice)
    await message.reply(TEXT[lang]['address_prompt'], reply_markup=kb_cancel_back(lang))
    await state.set_state(OrderForm.address)

# Handler for "Enter address manually" button in OrderForm.location state
@dp.message(OrderForm.location, F.text.in_([BTN['ru']['enter_address'], BTN['uz']['enter_address']]))
async def enter_addr_manual(message: types.Message, state: FSMContext):
    data = await state.get_data()
    lang = await get_user_lang(message.from_user.id, state)
    await state.update_data(location_lat=None, location_lon=None, address=None) # Reset location and address
    # Prompt for address
    await message.reply(TEXT[lang]['address_prompt'], reply_markup=kb_cancel_back(lang))
    await state.set_state(OrderForm.address)

# Handler for text input in OrderForm.location that is NOT a button
@dp.message(OrderForm.location, F.text)
async def handle_location_text_input(message: types.Message, state: FSMContext):
    data = await state.get_data()
    lang = await get_user_lang(message.from_user.id, state)
    # This handler fires if user sends text not matching location buttons.
    # Guide them back to expected input.
    await message.reply(TEXT[lang]['invalid_input'] + "\n\n" + TEXT[lang]['send_location'], reply_markup=kb_location(lang))


@dp.message(OrderForm.address, F.text) # Catches any text in this state (Back/Cancel buttons caught earlier)
async def handle_address_text(message: types.Message, state: FSMContext):
    data = await state.get_data()
    lang = await get_user_lang(message.from_user.id, state)
    addr = message.text.strip()

    if not addr:
        return await message.reply(TEXT[lang]['address_prompt']) # Repeat prompt if empty

    await state.update_data(address=addr) # Save address
    # Prompt for additional contact
    await message.reply(TEXT[lang]['additional_prompt'], reply_markup=kb_additional(lang))
    await state.set_state(OrderForm.additional)


@dp.message(OrderForm.additional, F.text) # Catches any text in this state (Skip/Back/Cancel buttons caught earlier)
async def handle_additional_text(message: types.Message, state: FSMContext):
    data = await state.get_data()
    lang = await get_user_lang(message.from_user.id, state)
    extra = message.text.strip()

    await state.update_data(additional_contact=extra) # Save additional contact

    # Prompt for quantity
    await message.reply(TEXT[lang]['input_quantity'].format(price=PRICE_PER_BOTTLE), reply_markup=kb_quantity(lang))
    await state.set_state(OrderForm.quantity)


@dp.message(OrderForm.quantity, F.text) # Catches any text in this state (Back/Cancel buttons caught earlier)
async def handle_quantity_text(message: types.Message, state: FSMContext):
    data = await state.get_data()
    lang = await get_user_lang(message.from_user.id, state)
    text = message.text.strip()

    # Validate input as a positive integer
    if not text.isdigit() or int(text) <= 0:
        err = TEXT[lang]['invalid_input'] + (" Enter a positive number." if lang == "ru" else " Iltimas, musbat raqam kiriting.")
        return await message.reply(err)

    qty = int(text)
    await state.update_data(quantity=qty) # Save quantity

    data = await state.get_data() # Get updated data, including quantity
    total = qty * PRICE_PER_BOTTLE

    uid = message.from_user.id
    user_info_db = {}
    try:
        # Get current name and username from DB for summary (more reliable than state)
        async with db.execute("SELECT name, contact, username FROM clients WHERE user_id=?", (uid,)) as cur:
            row = await cur.fetchone()
            if row:
                user_info_db = {"name": row[0], "contact": row[1], "username": row[2]}
    except Exception as e:
         logger.error(f"Error fetching client info {uid} for summary: {e}")
         # Fallback to state data if DB fetch fails
         user_info_db = {"name": data.get('name'), "contact": data.get('contact'), "username": message.from_user.username}


    display_name = user_info_db.get('name') or (TEXT[lang].get('not_specified', 'Not specified') if lang == 'ru' else 'Belgilangan emas')
    username = user_info_db.get('username', "")
    display_name_with_username = f"{display_name} (@{username})" if username else display_name
    contact_display = user_info_db.get('contact') or (TEXT[lang].get('not_specified', 'Not specified') if lang == 'ru' else 'Belgilangan emas')
    additional_contact_display = data.get('additional_contact') or ('–' if lang == 'ru' else '–')
    address_display = data.get('address') or (TEXT[lang].get('location_not_specified', 'Location not specified') if lang == 'ru' else 'Joylashuv belgilanmagan') # Show address or location placeholder


    summary = (
        f"{TEXT[lang]['order_summary']}\n\n"
        f"👤 {display_name_with_username}\n"
        f"📞 Основной: {contact_display}\n"
        f"📞 Доп.: {additional_contact_display}\n"
        f"📍 Адрес: {address_display}\n" # Show address or location placeholder
        f"🔢 Количество: {qty} " + ("шт" if lang == "ru" else "dona") + f" (Общая сумма: {total:,} " + ("сум" if lang == "ru" else "so'm") + ")\n"
    )

    # Confirmation keyboard
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅", callback_data="order_confirm")],
        [InlineKeyboardButton(text="❌", callback_data="order_cancel")]
    ])
    await message.reply(summary, reply_markup=kb)
    await state.set_state(OrderForm.confirm)

# --- Handlers for order confirmation inline buttons ---

@dp.callback_query(StateFilter(OrderForm.confirm), F.data == "order_confirm")
async def confirm_order(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    lang = await get_user_lang(callback.from_user.id, state)
    await callback.answer("✅ Подтверждено!" if lang == "ru" else "✅ Tasdiqlandi!")

    uid = callback.from_user.id

    contact = data.get("contact")
    additional_contact = data.get("additional_contact")
    location_lat = data.get("location_lat")
    location_lon = data.get("location_lon")
    address = data.get("address")
    quantity = data.get("quantity")

    # Final data validation before saving
    if not (contact and (address or (location_lat is not None and location_lon is not None)) and quantity is not None):
         logger.error(f"Missing data for order from user {uid}. State: {data}")
         error_message = TEXT[lang]['error_processing'] + " " + (TEXT[lang]['start_over'] if lang == "ru" else "Yangi boshlash tugmasini bosib qaytadan urinib ko'ring.")
         try:
             await callback.message.edit_text(callback.message.text + "\n\n" + error_message, reply_markup=None)
         except Exception: # If editing fails
             await bot.send_message(uid, error_message)
         await state.clear()
         await bot.send_message(uid, TEXT[lang]['back_to_main'], reply_markup=kb_main(lang, uid in ADMIN_CHAT_IDS, await is_user_registered(uid)))
         return

    now = datetime.now()
    order_time_str = now.strftime("%Y-%m-%d %H:%M:%S")
    localized_date_str = localize_date(now, 'ru') # Use RU for admin notification time

    order_id = None
    try:
        cursor = await db.cursor()
        # Initial status 'pending'
        await cursor.execute(
            "INSERT INTO orders(user_id, contact, additional_contact, location_lat, location_lon, address, quantity, order_time, status) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (uid, contact, additional_contact, location_lat, location_lon, address, quantity, order_time_str, 'pending')
        )
        await db.commit()
        order_id = cursor.lastrowid
        await cursor.close()
        logger.info(f"New order №{order_id} created by user {uid}")

    except Exception as e:
        logger.error(f"Error saving order to DB for user {uid}: {e}")
        error_message = TEXT[lang]['error_processing'] + " " + (TEXT[lang]['back_to_main'] if lang == "ru" else "Bosh menyuga qaytish.")
        try:
            await callback.message.edit_text(callback.message.text + "\n\n" + error_message, reply_markup=None)
        except Exception: # If editing fails
             await bot.send_message(uid, error_message)

        await state.clear()
        await bot.send_message(uid, TEXT[lang]['back_to_main'], reply_markup=kb_main(lang, uid in ADMIN_CHAT_IDS, await is_user_registered(uid)))
        return

    # Get current name and username from DB for admin notification (more reliable)
    user_info_db = {}
    try:
        async with db.execute("SELECT name, username FROM clients WHERE user_id=?", (uid,)) as cur:
            row = await cur.fetchone()
            if row:
                user_info_db = {"name": row[0], "username": row[1]}
    except Exception as e:
         logger.error(f"Error fetching client info {uid} for admin notification: {e}")
         # Fallback to state data if DB fetch fails
         user_info_db = {"name": data.get('name'), "username": callback.from_user.username}


    full_name = user_info_db.get('name', 'Не указан' if lang == 'ru' else 'Belgilangan emas')
    uname = user_info_db.get('username', "")
    display_name = f"{full_name} (@{uname})" if uname else full_name
    # Use contact from DB or state (already validated it exists)
    contact_display = data.get('contact') or user_info_db.get('contact', 'Не указан' if lang == 'ru' else 'Belgilangan emas')
    additional_contact_display = data.get('additional_contact') or ('–' if lang == 'ru' else '–')
    address_display = data.get('address') or (TEXT['ru'].get('location_not_specified', 'Location not specified') if location_lat is None else TEXT['ru'].get('location', 'Локация')) # Use RU for admin message

    total = quantity * PRICE_PER_BOTTLE

    msg_to_admin = (
        f"📣 <b>Новый заказ</b> (№{order_id})\n\n"
        f"👤 {display_name}\n"
        f"📞 Основной: {contact_display}\n"
        f"📞 Доп.: {additional_contact_display}\n"
        f"📍 Адрес: {address_display}\n" # Show address or "Location" if lat/lon exist
        f"🔢 Количество: {quantity} шт (Общая сумма: {total:,} сум)\n"
        f"⏰ Время заказа: {localized_date_str}\n" # Always RU for admin
        f"🆔 User ID: <code>{uid}</code>\n"
        f"✨ Статус: {STATUS_MAP['pending']['ru']}" # Initial status for admin is always in Russian in the notification
    )

    # Notify admins and group with inline status buttons (in Russian)
    # We assume admins prefer buttons in Russian
    admin_order_kb = kb_admin_order_status(order_id, 'ru') # Admin buttons are always in Russian

    all_recipients = set(ADMIN_CHAT_IDS)
    if GROUP_CHAT_ID is not None:
        all_recipients.add(GROUP_CHAT_ID)

    for chat_id in all_recipients:
        try:
            # Send text message first
            await bot.send_message(chat_id, msg_to_admin, parse_mode=ParseMode.HTML, reply_markup=admin_order_kb)
            # Then send location if available
            if location_lat is not None and location_lon is not None:
                await bot.send_location(chat_id, location_lat, location_lon)
        except Exception as e:
            # Log error if sending to a specific chat fails, but continue
            logger.error(f"Failed to send order notification {order_id} to chat {chat_id}: {e}")

    # Edit user's message
    try:
        await callback.message.edit_reply_markup(reply_markup=None) # Remove inline buttons
        # Append confirmation text to the existing summary
        await callback.message.edit_text(callback.message.text + "\n\n" + TEXT[lang]['order_confirmed'], reply_markup=None)
    except Exception as e:
         logger.warning(f"Failed to edit message after order confirmation {order_id} for user {uid}: {e}")
         # Send a new message if editing fails
         await bot.send_message(uid, TEXT[lang]['order_confirmed'], reply_markup=None)

    # Clear state and return to main menu for the user
    is_registered = await is_user_registered(uid)
    await bot.send_message(uid, TEXT[lang]['back_to_main'], reply_markup=kb_main(lang, uid in ADMIN_CHAT_IDS, is_registered))
    await state.clear()


@dp.callback_query(StateFilter(OrderForm.confirm), F.data == "order_cancel")
async def cancel_order_callback(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    lang = await get_user_lang(callback.from_user.id, state)
    await callback.answer("❌ Отменено" if lang == "ru" else "❌ Bekor qilindi")

    # Edit user's message to indicate cancellation
    try:
        await callback.message.edit_reply_markup(reply_markup=None) # Remove inline buttons
        # Append cancellation text to the existing summary
        await callback.message.edit_text(callback.message.text + "\n\n" + TEXT[lang]['order_cancelled'], reply_markup=None)
    except Exception as e:
        logger.warning(f"Failed to edit message after order cancellation for user {callback.from_user.id}: {e}")
        # Send a new message if editing fails
        await bot.send_message(callback.from_user.id, TEXT[lang]['order_cancelled'], reply_markup=None)

    # Clear state and return to main menu for the user
    uid = callback.from_user.id
    is_registered = await is_user_registered(uid)
    await bot.send_message(uid, TEXT[lang]['back_to_main'], reply_markup=kb_main(lang, uid in ADMIN_CHAT_IDS, is_registered))
    await state.clear()

# --- Helper function to cancel process ---
async def cancel_process(message: types.Message, state: FSMContext):
    uid = message.from_user.id
    lang = await get_user_lang(uid, state)

    is_registered = await is_user_registered(uid)

    await state.clear()
    await message.reply(TEXT[lang]['process_cancelled'], reply_markup=kb_main(lang, uid in ADMIN_CHAT_IDS, is_registered))


# --- Default handler (catches all other text messages) ---
# This handler must be registered LAST
@dp.message(F.text)
async def default_text_handler(message: types.Message, state: FSMContext):
    uid = message.from_user.id
    lang = await get_user_lang(uid, state)

    is_registered = await is_user_registered(uid)

    # Check if the user is in the language selection state
    current_state = await state.get_state()
    if current_state == LangSelect.choosing.state:
        # If user sent invalid text in language selection state
        await message.reply(TEXT[lang]['invalid_input'] + " " + TEXT['ru']['choose_language'], reply_markup=kb_language_select()) # Always show RU text for lang selection prompt
        return

    # If user is not in language selection or another FSM state,
    # or if it's just garbage input outside of states.
    # kb_main will be built considering user registration status.
    await message.reply(TEXT[lang]['invalid_input'] + " " + TEXT[lang]['back_to_main'], reply_markup=kb_main(lang, uid in ADMIN_CHAT_IDS, is_registered))
    await state.clear()


# Default handler for content types other than text (stickers, audio, video, etc.)
# Place this before the text handler if possible, or ensure text handler doesn't catch these.
@dp.message(~F.text & ~F.content_type.in_([ContentType.CONTACT, ContentType.LOCATION, ContentType.PHOTO]))
async def default_other_handler(message: types.Message, state: FSMContext):
    uid = message.from_user.id
    lang = await get_user_lang(uid, state)

    is_registered = await is_user_registered(uid)

    # Check state in case this content type is expected in a specific state
    # For this bot, contact, location, photo are handled specifically.
    # Other types are unexpected.
    current_state = await state.get_state()
    if current_state == LangSelect.choosing.state:
         # If non-text sent in language selection state
         await message.reply(TEXT[lang]['invalid_input'] + " " + TEXT['ru']['choose_language'], reply_markup=kb_language_select())
         return
    # If in any other FSM state where this content type is not handled specifically,
    # or if outside FSM states.
    await message.reply(TEXT[lang]['invalid_input'] + " " + TEXT[lang]['back_to_main'], reply_markup=kb_main(lang, uid in ADMIN_CHAT_IDS, is_registered))
    await state.clear()


# --- Database Initialization ---
async def init_db():
    """Initializes the database (creates tables if they don't exist)."""
    logger.info("Initializing database...")
    if db is None:
         logger.error("Database connection not established in init_db.")
         # This should not happen if main() runs correctly, but defensive check
         return
    try:
        await db.execute('''
            CREATE TABLE IF NOT EXISTS clients (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                contact TEXT,
                name TEXT,
                language TEXT
            )
        ''')
        await db.execute('''
            CREATE TABLE IF NOT EXISTS orders (
                order_id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                contact TEXT,
                additional_contact TEXT,
                location_lat REAL,
                location_lon REAL,
                address TEXT,
                quantity INTEGER,
                order_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                status TEXT, -- Use short key: 'pending', 'accepted', 'in_progress', 'completed', 'rejected'
                FOREIGN KEY (user_id) REFERENCES clients (user_id) ON DELETE CASCADE
            )
        ''')
        await db.commit()
        logger.info("Database initialized.")
    except Exception as e:
         logger.critical(f"Error initializing DB: {e}")
         # Critical failure: cannot proceed without a database
         exit(1)


# --- Main function to run the bot with webhook ---
async def main():
    global db
    logger.info(f"Connecting to database at {DATABASE_PATH}...")
    try:
        # Use the configured DATABASE_PATH
        db = await aiosqlite.connect(DATABASE_PATH)
        db.row_factory = aiosqlite.Row
        logger.info("Database connection successful.")
        await init_db() # Initialize tables if they don't exist
    except Exception as e:
        logger.critical(f"Critical error connecting or initializing DB: {e}")
        # Critical failure: bot cannot work without DB
        exit(1)

    logger.info("Starting bot with Webhook...")
    # Log configuration values for debugging on Render
    logger.info(f"ADMIN_CHAT_IDS: {ADMIN_CHAT_IDS}")
    logger.info(f"GROUP_CHAT_ID: {GROUP_CHAT_ID}")
    logger.info(f"PRICE_PER_BOTTLE: {PRICE_PER_BOTTLE}")
    logger.info(f"WEBHOOK_URL: {WEBHOOK_URL}")
    logger.info(f"WEBHOOK_PATH: {WEBHOOK_SECRET_PATH}")
    logger.info(f"WEBAPP_HOST: {WEBAPP_HOST}")
    logger.info(f"WEBAPP_PORT: {WEBAPP_PORT}")
    logger.info(f"DATABASE_PATH: {DATABASE_PATH}")

    try:
        # Set webhook URL in Telegram
        logger.info(f"Setting webhook URL to {WEBHOOK_URL}...")
        # Use secret_token for additional security - verify it in SimpleRequestHandler
        await bot.set_webhook(url=WEBHOOK_URL, secret_token=WEBHOOK_SECRET_PATH)
        logger.info("Webhook successfully set.")

        # Configure webhook handler
        webhook_request_handler = SimpleRequestHandler(
            dispatcher=dp,
            bot=bot,
            secret_token=WEBHOOK_SECRET_PATH # Verify secret token from incoming updates
        )

        # Create aiohttp web application
        app = web.Application()

        # Add the webhook handler route. Telegram POSTs updates to this path.
        app.router.add_post(WEBHOOK_SECRET_PATH, webhook_request_handler)

        # Add a health check endpoint (recommended for Render Web Services)
        # Render uses this to check if your service is alive.
        async def health_check(request):
            # You could add DB check here too if needed: await db.execute("SELECT 1")
            return web.Response(status=200, text="OK")

        app.router.add_get("/health", health_check) # Render Health Check path
        logger.info("Health check endpoint /health added.")


        # Setup Aiogram with the aiohttp application
        setup_application(app, dp, bot=bot)

        # Register your handlers BEFORE starting the web server runner
        # Order matters: Specific FSM -> General Buttons -> Defaults

        # 1. Commands (/start)
        dp.message.register(cmd_start, Command("start"))

        # 2. FSM-specific button handlers (Cancel, Back, Skip)
        dp.message.register(handle_cancel_btn, StateFilter(OrderForm), F.text.in_([BTN['ru']['cancel'], BTN['uz']['cancel']]))
        dp.message.register(handle_back_btn, StateFilter(OrderForm.address, OrderForm.additional, OrderForm.quantity), F.text.in_([BTN['ru']['back'], BTN['uz']['back']]))
        dp.message.register(handle_skip_btn, OrderForm.additional, F.text.in_([BTN['ru']['skip'], BTN['uz']['skip']]))

        # 3. FSM handlers by content type/text for specific states
        dp.message.register(process_lang, LangSelect.choosing, F.text.in_(["🇷🇺 Русский", "🇺🇿 Ўзбек"]))
        dp.message.register(reg_contact, OrderForm.contact, F.content_type == "contact")
        dp.message.register(prompt_contact_again, OrderForm.contact) # Catches other input in contact state
        dp.message.register(reg_name_text, OrderForm.name, F.content_type == "text")
        dp.message.register(reg_name_photo, OrderForm.name, F.content_type == "photo")
        dp.message.register(loc_received, OrderForm.location, F.content_type == "location")
        dp.message.register(enter_addr_manual, OrderForm.location, F.text.in_([BTN['ru']['enter_address'], BTN['uz']['enter_address']]))
        dp.message.register(handle_location_text_input, OrderForm.location, F.text) # Catches invalid text in location state

        dp.message.register(handle_address_text, OrderForm.address, F.text)
        dp.message.register(handle_additional_text, OrderForm.additional, F.text)
        dp.message.register(handle_quantity_text, OrderForm.quantity, F.text)

        # 4. Inline button handlers (order confirmation/cancellation, admin status, admin confirms)
        dp.callback_query.register(confirm_order, StateFilter(OrderForm.confirm), F.data == "order_confirm")
        dp.callback_query.register(cancel_order_callback, StateFilter(OrderForm.confirm), F.data == "order_cancel")
        dp.callback_query.register(handle_admin_clear_callback, AdminStates.main, F.data.startswith("admin_clear_"))
        dp.callback_query.register(handle_confirm_clear_clients, AdminStates.confirm_clear_clients, F.data.startswith("admin_confirm_clients_"))
        dp.callback_query.register(handle_confirm_clear_orders, AdminStates.confirm_clear_orders, F.data.startswith("admin_confirm_orders_"))
        dp.callback_query.register(handle_admin_set_status, F.data.startswith("set_status:")) # Admin status handler

        # 5. General button handlers (My Orders, Change Lang, Start Over, Manage DB)
        dp.message.register(handle_start_over_btn, F.text.in_([BTN['ru']['start_over'], BTN['uz']['start_over']]))
        dp.message.register(handle_change_lang_btn, F.text.in_([TEXT['ru']['change_lang'], TEXT['uz']['change_lang']]))
        dp.message.register(handle_my_orders_btn, F.text.in_([BTN['ru']['my_orders'], BTN['uz']['my_orders']]))
        dp.message.register(handle_edit_order_btn, F.text.in_([BTN['ru']['edit_order'], BTN['uz']['edit_order']])) # Placeholder
        dp.message.register(handle_manage_db_btn, F.text.in_([BTN['ru']['manage_db'], BTN['uz']['manage_db']])) # Admin only

        # 6. Default handlers (catching everything else) - place LAST
        # Non-text first
        dp.message.register(default_other_handler, ~F.text & ~F.content_type.in_([ContentType.CONTACT, ContentType.LOCATION, ContentType.PHOTO]))
        # Text last
        dp.message.register(default_text_handler, F.text)


        # Start the aiohttp web server runner
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, host=WEBAPP_HOST, port=int(WEBAPP_PORT)) # Cast port to int
        logger.info(f"Starting web server on {WEBAPP_HOST}:{WEBAPP_PORT} for webhook path {WEBHOOK_SECRET_PATH}")
        await site.start()

        # The web server will run until the process is stopped.
        # Use asyncio.Event().wait() to keep the main coroutine alive.
        await asyncio.Event().wait()

    except Exception as e:
        logger.error(f"Error during webhook server startup or operation: {e}")
    finally:
        # Clean up webhook in Telegram and close DB connection on shutdown
        logger.info("Shutting down...")
        # Try to delete webhook gracefully
        try:
             await bot.delete_webhook()
             logger.info("Webhook deleted.")
        except Exception as e:
             logger.warning(f"Failed to delete webhook on shutdown: {e}")

        # Close DB connection
        if db:
            await db.close()
            logger.info("Database connection closed.")
        logger.info("Bot stopped.")


if __name__ == "__main__":
    # Basic checks before running asyncio loop
    if not API_TOKEN or not WEBHOOK_HOST or not WEBAPP_PORT or not WEBHOOK_SECRET_PATH:
         logger.critical("Missing required environment variables for webhook setup. Exiting.")
         exit(1) # Exit if critical config is missing

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped manually by KeyboardInterrupt")
    except Exception as e:
        logger.critical(f"Bot stopped with an unhandled exception: {e}", exc_info=True)