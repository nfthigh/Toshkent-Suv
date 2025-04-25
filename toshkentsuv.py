import logging, asyncio, aiosqlite, re
from datetime import datetime
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.enums import ParseMode, ChatType, ContentType

# !!! ВАЖНО: Замените эти значения на ваши реальные !!!
API_TOKEN = '5647678711:AAHYnN64A-1OdDtzSUGZ4F6i_-MQhNHke3M'
ADMIN_CHAT_IDS = [7880940719,5366741102,  ] # Список ID администраторов (может быть несколько)
GROUP_CHAT_ID = -4623233228 # ID чата для уведомлений о новых заказах (может быть группой или каналом)
# !!! Конец важных значений !!!

PRICE_PER_BOTTLE = 16000

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

bot = Bot(token=API_TOKEN, timeout=60)
storage = MemoryStorage() # Используем MemoryStorage для простоты; для продакшена рассмотрите FileStorage или RedisStorage
dp = Dispatcher(storage=storage)
db: aiosqlite.Connection = None  # Глобальное соединение; инициализируется в main()


# --- Вспомогательные функции ---
def fmt_phone(num: str) -> str:
    """Форматирует номер телефона, удаляя лишние символы."""
    cleaned_num = re.sub(r'[^\d+]', '', num)
    return cleaned_num

def localize_date(dt: datetime, lang: str) -> str:
    """Локализует дату и время в заданном формате."""
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
    Получает язык пользователя из состояния FSM, затем из БД.
    Если state не предоставлен или язык не найден нигде, возвращает 'ru'.
    Также сохраняет язык в state, если он найден в БД и state предоставлен.
    """
    if state:
        data = await state.get_data()
        if 'language' in data:
            return data['language']
    # Если языка нет в state или state не предоставлен, попробуем из БД
    if db:
        try:
            async with db.execute("SELECT language FROM clients WHERE user_id=?", (user_id,)) as cur:
                row = await cur.fetchone()
                if row and row[0]:
                    # Если нашли в БД, сохраним в state для будущих быстрых доступов
                    if state: await state.update_data(language=row[0])
                    return row[0]
        except Exception as e:
            logger.error(f"Error getting language from DB for user {user_id}: {e}")

    # Если язык не найден ни в state, ни в БД
    if state: await state.update_data(language='ru') # Сохранить дефолтный в state
    return 'ru'


async def is_user_registered(user_id: int) -> bool:
    """Проверяет, зарегистрирован ли пользователь (наличие записи и заполненного имени в БД)."""
    if db is None:
         logger.error("Соединение с БД не установлено в is_user_registered.")
         return False
    try:
        async with db.execute("SELECT name FROM clients WHERE user_id=?", (user_id,)) as cur:
            row = await cur.fetchone()
            # Считаем пользователя зарегистрированным, если есть запись и поле 'name' не пустое
            return bool(row and row[0])
    except Exception as e:
        logger.error(f"Ошибка при проверке регистрации пользователя {user_id}: {e}")
        return False # Предполагаем, что не зарегистрирован в случае ошибки

# --- FSM States ---
class LangSelect(StatesGroup):
    choosing = State()

class OrderForm(StatesGroup):
    contact = State()
    name = State()
    location = State() # Может содержать геолокацию или признак ручного ввода
    address = State() # Текстовый адрес, обязателен после location
    additional = State()
    quantity = State()
    confirm = State()

class AdminStates(StatesGroup):
    main = State() # Главное меню админа
    confirm_clear_clients = State()
    confirm_clear_orders = State()
    # Возможно, добавить состояние для управления заказами, если оно станет сложным

# --- Локализованные тексты и кнопки ---
TEXT = {
    'ru': {
        'choose_language': "Выберите язык:",
        'welcome': "👋 Добро пожаловать, {name}!",
        'greeting_prompt': "👋 Добро пожаловать, {name}!\n\n", # Добавлен отдельный текст для приветствия перед следующим шагом
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
        'change_lang': "🔄 Сменить язык", # Текст кнопки смены языка
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
        'greeting_prompt': "👋 Xush kelibsiz, {name}!\n\n", # Добавлен отдельный текст
        'send_contact': "Boshlash uchun, iltimas, telefon raqamingizni yuboring.",
        'prompt_contact': "Iltimos, raqamingizni yuborish uchun '📞 Kontaktni yuborish' tugmasini bosing.",
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
        'change_lang': "🔄 Tilni almashtirish", # Текст кнопки смены языка
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
        'error_processing': "So'rovingizni qayta ishlashda xatolik yuz berdi. Iltimos, qaytadan urinib ko'ring yoki qo'llab-quvvatlash xizmati bilan bog'laning.",
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

# Маппинг ключей статусов к текстам
STATUS_MAP = {
    'pending': {'ru': TEXT['ru']['status_pending'], 'uz': TEXT['uz']['status_pending']},
    'accepted': {'ru': TEXT['ru']['status_accepted'], 'uz': TEXT['uz']['status_accepted']},
    'in_progress': {'ru': TEXT['ru']['status_in_progress'], 'uz': TEXT['uz']['status_in_progress']},
    'completed': {'ru': TEXT['ru']['status_completed'], 'uz': TEXT['uz']['status_completed']},
    'rejected': {'ru': TEXT['ru']['status_rejected'], 'uz': TEXT['uz']['status_rejected']},
}

# Маппинг ключей статусов для колбэков (чтобы из accept получить accepted)
ADMIN_STATUS_CALLBACK_MAP = {
    'accept': 'accepted', # Принять -> Принят
    'reject': 'rejected', # Отменить -> Отменен
    'complete': 'completed', # Выполнить -> Выполнен
}


BTN = {
    'ru': {
        'send_contact': "📞 Отправить контакт",
        'cancel': "❌ Отменить",
        'send_location': "📍 Отправить локацию",
        'enter_address': "🏠 Ввести адрес вручную",
        'start_over': "🔄 Начать сначала",
        'my_orders': "📦 Мои заказы",
        'edit_order': "✏️ Редактировать заказ", # Пока не реализована
        'manage_db': "🔧 Управление базой данных", # Только для админов
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
        'edit_order': "✏️ Buyurtmani tahrirlash", # Пока не реализована
        'manage_db': "🔧 Bazani boshqarish", # Только для админов
        'skip': "O'tkazib yuborish",
        'back': "⬅️ Orqaga",
        # Admin buttons (inline) - text for buttons shown to ADMIN (use TEXT dict)
        'admin_clear_clients': "🗑️ Mijozlarni tozalash",
        'admin_clear_orders': "🗑️ Buyurtmalarni tozalash",
        'admin_confirm_yes': "✅ Ha",
        'admin_confirm_no': "❌ Yo'q",
    }
}


# --- Функции формирования клавиатур ---
def kb_main(lang, is_admin=False, is_registered=False):
    """Главное меню"""
    kb = []
    if not is_registered:
        kb.append([KeyboardButton(text=BTN[lang]['send_contact'], request_contact=True)])

    kb.append([KeyboardButton(text=BTN[lang]['my_orders'])])

    # Кнопка "Редактировать заказ" пока не реализована
    # if is_registered:
    #    kb.append([KeyboardButton(text=BTN[lang]['edit_order'])])

    kb.append([KeyboardButton(text=BTN[lang]['start_over'])])

    if is_admin:
        kb.append([KeyboardButton(text=BTN[lang]['manage_db'])])

    kb.append([KeyboardButton(text=TEXT[lang]['change_lang'])])

    return ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)

def kb_location(lang):
    """Клавиатура для выбора локации/ввода адреса"""
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=BTN[lang]['send_location'], request_location=True)],
            [KeyboardButton(text=BTN[lang]['enter_address'])],
            [KeyboardButton(text=BTN[lang]['cancel'])]
        ],
        resize_keyboard=True
    )

def kb_cancel_back(lang):
    """Клавиатура с кнопками Отмена и Назад"""
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=BTN[lang]['back'])],
            [KeyboardButton(text=BTN[lang]['cancel'])]
        ],
        resize_keyboard=True
    )

def kb_additional(lang):
    """Клавиатура для доп. контакта с Пропустить, Назад, Отмена"""
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=BTN[lang]['skip'])],
            [KeyboardButton(text=BTN[lang]['back'])],
            [KeyboardButton(text=BTN[lang]['cancel'])]
        ],
        resize_keyboard=True
    )

def kb_quantity(lang):
    """Клавиатура для ввода количества с Назад, Отмена"""
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=BTN[lang]['back'])],
            [KeyboardButton(text=BTN[lang]['cancel'])]
        ],
        resize_keyboard=True
    )

def kb_language_select():
    """Клавиатура выбора языка"""
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text='🇷🇺 Русский'), KeyboardButton(text='🇺🇿 Ўзбек')]],
        resize_keyboard=True
    )

def kb_admin_db(lang):
    """Инлайн клавиатура для админских действий с БД"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=BTN[lang]['admin_clear_clients'], callback_data="admin_clear_clients")],
        [InlineKeyboardButton(text=BTN[lang]['admin_clear_orders'], callback_data="admin_clear_orders")],
    ])

def kb_admin_confirm(lang, action_type):
    """Инлайн клавиатура подтверждения админского действия"""
    # action_type будет либо 'clients', либо 'orders'
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text=BTN[lang]['admin_confirm_yes'], callback_data=f"admin_confirm_{action_type}_yes"),
            InlineKeyboardButton(text=BTN[lang]['admin_confirm_no'], callback_data=f"admin_confirm_{action_type}_no")
        ]
    ])

def kb_admin_order_status(order_id: int, lang: str) -> InlineKeyboardMarkup:
    """Инлайн клавиатура для изменения статуса заказа админами."""
    # Кнопки должны отправлять callback_data в формате "set_status:<order_id>:<status_key>"
    # Кнопки для админов обычно на одном языке (например, русском) для удобства админского чата.
    # В данном случае, берем текст кнопок из TEXT[lang] на языке админа.
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

# --- Хэндлеры общих кнопок (работают независимо от состояния или в определенных состояниях) ---

# Хэндлер для кнопки "Отменить" (работает в любых состояниях OrderForm)
@dp.message(StateFilter(OrderForm), F.text.in_([BTN['ru']['cancel'], BTN['uz']['cancel']]))
async def handle_cancel_btn(message: types.Message, state: FSMContext):
    await cancel_process(message, state)

# Хэндлер для кнопки "Назад" (работает в определенных состояниях OrderForm)
@dp.message(StateFilter(OrderForm.address, OrderForm.additional, OrderForm.quantity), F.text.in_([BTN['ru']['back'], BTN['uz']['back']]))
async def handle_back_btn(message: types.Message, state: FSMContext):
    data = await state.get_data()
    lang = await get_user_lang(message.from_user.id, state)

    current_state = await state.get_state()

    if current_state == OrderForm.address.state:
        # Из address назад в location
        await message.reply(TEXT[lang]['send_location'], reply_markup=kb_location(lang))
        await state.set_state(OrderForm.location)
        await state.update_data(address=None) # Сбрасываем введенный адрес
    elif current_state == OrderForm.additional.state:
        # Из additional назад в address
        await message.reply(TEXT[lang]['address_prompt'], reply_markup=kb_cancel_back(lang))
        await state.set_state(OrderForm.address)
        await state.update_data(additional_contact=None) # Сбрасываем доп. контакт
    elif current_state == OrderForm.quantity.state:
        # Из quantity назад в additional
        await message.reply(TEXT[lang]['additional_prompt'], reply_markup=kb_additional(lang))
        await state.set_state(OrderForm.additional)
        await state.update_data(quantity=None) # Сбрасываем количество


# Хэндлер для кнопки "Пропустить" (работает в состоянии OrderForm.additional)
@dp.message(OrderForm.additional, F.text.in_([BTN['ru']['skip'], BTN['uz']['skip']]))
async def handle_skip_btn(message: types.Message, state: FSMContext):
    data = await state.get_data()
    lang = await get_user_lang(message.from_user.id, state)
    await state.update_data(additional_contact=None) # Сохраняем как None
    await message.reply(TEXT[lang]['input_quantity'].format(price=PRICE_PER_BOTTLE), reply_markup=kb_quantity(lang))
    await state.set_state(OrderForm.quantity)

# Хэндлер для кнопки "Начать сначала" (работает в любом состоянии)
@dp.message(F.text.in_([BTN['ru']['start_over'], BTN['uz']['start_over']]))
async def handle_start_over_btn(message: types.Message, state: FSMContext):
    await cmd_start(message, state) # По сути, перезапускает процесс как команда /start

# Хэндлер для кнопки "Сменить язык" (работает в любом состоянии)
@dp.message(F.text.in_([TEXT['ru']['change_lang'], TEXT['uz']['change_lang']]))
async def handle_change_lang_btn(message: types.Message, state: FSMContext):
    await state.clear() # Сбрасываем текущее состояние (в т.ч. заказ)
    await message.reply(TEXT['ru']['choose_language'], reply_markup=kb_language_select())
    await state.set_state(LangSelect.choosing)

# Хэндлер для кнопки "Мои заказы" (работает в любом состоянии)
@dp.message(F.text.in_([BTN['ru']['my_orders'], BTN['uz']['my_orders']]))
async def handle_my_orders_btn(message: types.Message, state: FSMContext):
    uid = message.from_user.id
    lang = await get_user_lang(uid, state)

    is_registered = await is_user_registered(uid)

    if not is_registered:
         await message.reply(TEXT[lang]['access_denied'], reply_markup=kb_main(lang, uid in ADMIN_CHAT_IDS, is_registered))
         await state.clear()
         return

    try:
        async with db.execute("SELECT order_id, order_time, quantity, status, address, location_lat, location_lon FROM orders WHERE user_id=? ORDER BY order_time DESC", (uid,)) as cur:
            orders = await cur.fetchall()
    except Exception as e:
        logger.error(f"Ошибка при получении заказов для пользователя {uid}: {e}")
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
            logger.warning(f"Ошибка парсинга даты для заказа {order_id}: {order_time_str}")
            localized_order_time = order_time_str

        # Получаем локализованный текст статуса
        localized_status = STATUS_MAP.get(status_key, {}).get(lang, status_key)

        # Определяем, как показать адрес/локацию
        display_address = address if address else (TEXT[lang].get('location', 'Локация/Joylashuv') if lat is not None else TEXT[lang].get('not_specified', 'Не указан/Belgilangan emas'))


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


# Хэндлер для кнопки "Редактировать заказ" (пока заглушка)
@dp.message(F.text.in_([BTN['ru']['edit_order'], BTN['uz']['edit_order']]))
async def handle_edit_order_btn(message: types.Message, state: FSMContext):
    uid = message.from_user.id
    lang = await get_user_lang(uid, state)

    is_registered = await is_user_registered(uid)

    if is_registered:
        await message.reply(TEXT[lang]['feature_not_implemented'], reply_markup=kb_main(lang, uid in ADMIN_CHAT_IDS, True))
        await state.clear()
    else:
        await message.reply(TEXT[lang]['access_denied'], reply_markup=kb_main(lang, uid in ADMIN_CHAT_IDS, False))
        await state.clear()


# --- Хэндлеры админских кнопок ---

# Хэндлер для кнопки "Управление базой данных"
@dp.message(F.text.in_([BTN['ru']['manage_db'], BTN['uz']['manage_db']]))
async def handle_manage_db_btn(message: types.Message, state: FSMContext):
    uid = message.from_user.id
    lang = await get_user_lang(uid, state) # Используем язык админа

    if uid not in ADMIN_CHAT_IDS:
        await message.reply(TEXT[lang]['access_denied'], reply_markup=kb_main(lang, False, await is_user_registered(uid)))
        await state.clear()
        return

    await message.reply(TEXT[lang]['choose_admin_action'], reply_markup=kb_admin_db(lang))
    await state.set_state(AdminStates.main)


# Хэндлеры инлайн-кнопок админских действий (очистка)
@dp.callback_query(AdminStates.main, F.data.startswith("admin_clear_"))
async def handle_admin_clear_callback(callback: types.CallbackQuery, state: FSMContext):
    await callback.answer()
    uid = callback.from_user.id
    lang = await get_user_lang(uid, state) # Используем язык админа

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
        await callback.message.edit_text(TEXT[lang]['invalid_input'], reply_markup=None)
        await state.clear()
        return

    await callback.message.edit_text(confirm_text, reply_markup=confirm_kb)


# Хэндлер подтверждения очистки клиентов
@dp.callback_query(AdminStates.confirm_clear_clients, F.data.startswith("admin_confirm_clients_"))
async def handle_confirm_clear_clients(callback: types.CallbackQuery, state: FSMContext):
    await callback.answer()
    uid = callback.from_user.id
    lang = await get_user_lang(uid, state) # Используем язык админа

    action = callback.data.split('_')[-1]

    if action == 'yes':
        try:
            await db.execute("DELETE FROM clients") # С CASCADE orders удалятся тоже
            await db.commit()
            await callback.message.edit_text(TEXT[lang]['db_clients_cleared'], reply_markup=None)
            logger.info(f"Админ {uid} очистил базу клиентов (и заказов).")
        except Exception as e:
            logger.error(f"Ошибка при очистке клиентов/заказов (админ {uid}): {e}")
            await callback.message.edit_text(f"{TEXT[lang]['error_processing']} Ошибка: {e}", reply_markup=None)
    else: # action == 'no'
        await callback.message.edit_text(TEXT[lang]['action_cancelled'], reply_markup=None)
        logger.info(f"Админ {uid} отменил очистку клиентов.")

    await state.clear()
    is_registered = await is_user_registered(uid)
    await bot.send_message(uid, TEXT[lang]['back_to_main'], reply_markup=kb_main(lang, uid in ADMIN_CHAT_IDS, is_registered))


# Хэндлер подтверждения очистки заказов
@dp.callback_query(AdminStates.confirm_clear_orders, F.data.startswith("admin_confirm_orders_"))
async def handle_confirm_clear_orders(callback: types.CallbackQuery, state: FSMContext):
    await callback.answer()
    uid = callback.from_user.id
    lang = await get_user_lang(uid, state) # Используем язык админа

    action = callback.data.split('_')[-1]

    if action == 'yes':
        try:
            await db.execute("DELETE FROM orders")
            await db.commit()
            await callback.message.edit_text(TEXT[lang]['db_orders_cleared'], reply_markup=None)
            logger.info(f"Админ {uid} очистил базу заказов.")
        except Exception as e:
            logger.error(f"Ошибка при очистке заказов (админ {uid}): {e}")
            await callback.message.edit_text(f"{TEXT[lang]['error_processing']} Ошибка: {e}", reply_markup=None)
    else: # action == 'no'
        await callback.message.edit_text(TEXT[lang]['action_cancelled'], reply_markup=None)
        logger.info(f"Админ {uid} отменил очистку заказов.")

    await state.clear()
    is_registered = await is_user_registered(uid)
    await bot.send_message(uid, TEXT[lang]['back_to_main'], reply_markup=kb_main(lang, uid in ADMIN_CHAT_IDS, is_registered))


# --- Хэндлер изменения статуса заказа админом ---
@dp.callback_query(F.data.startswith("set_status:"))
async def handle_admin_set_status(callback: types.CallbackQuery, state: FSMContext):
    uid = callback.from_user.id
    lang = await get_user_lang(uid, state) # Язык админа
    admin_name = callback.from_user.full_name
    admin_username = callback.from_user.username or "N/A"


    if uid not in ADMIN_CHAT_IDS:
        await callback.answer(TEXT[lang]['access_denied'], show_alert=True)
        return

    try:
        # Парсим callback_data: set_status:<order_id>:<status_key>
        parts = callback.data.split(':')
        if len(parts) != 3:
            await callback.answer(TEXT[lang]['invalid_input'], show_alert=True)
            return
        order_id = int(parts[1])
        action_key = parts[2] # 'accept', 'reject', 'complete'
        new_status_key = ADMIN_STATUS_CALLBACK_MAP.get(action_key)

        if not new_status_key:
            await callback.answer(TEXT[lang]['invalid_input'], show_alert=True)
            return

        # Получаем текущий статус, user_id клиента и все данные заказа для сводки
        async with db.execute("SELECT user_id, status, contact, additional_contact, address, quantity, order_time, location_lat, location_lon FROM orders WHERE order_id=?", (order_id,)) as cur:
            order_row = await cur.fetchone()

        if not order_row:
            await callback.answer(TEXT[lang]['order_not_found'].format(order_id=order_id), show_alert=True)
            try:
                await callback.message.edit_reply_markup(reply_markup=None) # Убрать кнопки, если заказ не найден
            except Exception as e:
                logger.warning(f"Не удалось убрать кнопки с сообщения о заказе {order_id}: {e}")
            return

        client_id, current_status_key, contact, additional_contact, address, quantity, order_time_str, lat, lon = order_row

        # Проверяем, не является ли текущий статус финальным
        final_statuses = ['completed', 'rejected'] # Ключи финальных статусов
        if current_status_key in final_statuses:
            await callback.answer(TEXT[lang]['order_already_finalized'].format(order_id=order_id, status=STATUS_MAP.get(current_status_key,{}).get(lang, current_status_key)), show_alert=True)
            # Убираем кнопки, если статус финальный
            try:
                await callback.message.edit_reply_markup(reply_markup=None)
            except Exception as e:
                 logger.warning(f"Не удалось убрать кнопки с финального заказа {order_id} в чате {callback.message.chat.id}: {e}")
            return

        # Обновляем статус в БД
        await db.execute("UPDATE orders SET status=? WHERE order_id=?", (new_status_key, order_id))
        await db.commit()
        logger.info(f"Заказ №{order_id} статус обновлен на '{new_status_key}' админом {uid}")

        # Получаем локализованный текст нового статуса для админа
        new_status_text_admin = STATUS_MAP.get(new_status_key, {}).get(lang, new_status_key)


        # Редактируем сообщение в админском чате/группе
        try:
            # Добавляем информацию об админе, который поменял статус
            log_message = TEXT[lang]['admin_status_update_log'].format(
                order_id=order_id,
                status=new_status_text_admin, # Используем локализованный текст для админа
                admin_name=admin_name,
                admin_username=admin_username
            )
            # Обновляем текст сообщения, добавляя лог и новый статус
            # Парсим существующий текст сообщения до лога
            current_text = callback.message.text
            log_start_index = current_text.find("<i>")
            if log_start_index != -1:
                 # Обрезаем старые логи, чтобы не накапливались
                 base_text = current_text[:log_start_index].strip()
            else:
                 base_text = current_text.strip()

            # Находим строку со статусом и заменяем ее
            # Пример: "✨ Статус: Ожидание обработки"
            status_line_pattern = re.compile(r"✨ Статус: .*")
            if status_line_pattern.search(base_text):
                 base_text = status_line_pattern.sub(f"✨ Статус: {new_status_text_admin}", base_text)
            else:
                 # Если строка статуса не найдена, просто добавляем ее в конец базового текста
                 base_text += f"\n✨ Статус: {new_status_text_admin}"


            updated_text = f"{base_text}\n\n<i>{log_message}</i>"


            await callback.message.edit_text(
                updated_text,
                parse_mode=ParseMode.HTML,
                reply_markup=None # Убираем кнопки после обработки
            )
        except Exception as e:
            logger.error(f"Не удалось отредактировать сообщение о заказе {order_id} в чате {callback.message.chat.id}: {e}")
            # Если не удалось отредактировать, отправляем новое лог-сообщение
            try:
                 # Повторно получаем язык админа, если state был очищен
                 current_admin_lang = await get_user_lang(uid)
                 retry_log_message = TEXT[current_admin_lang]['admin_status_update_log'].format(
                    order_id=order_id,
                    status=STATUS_MAP.get(new_status_key, {}).get(current_admin_lang, new_status_key),
                    admin_name=admin_name,
                    admin_username=admin_username
                 )
                 await bot.send_message(callback.message.chat.id, f"<i>{retry_log_message}</i>", parse_mode=ParseMode.HTML)
            except Exception as e2:
                 logger.error(f"Не удалось отправить лог-сообщение об обновлении статуса заказа {order_id}: {e2}")


        # Уведомляем клиента об изменении статуса
        client_lang = await get_user_lang(client_id) # Получаем язык клиента
        client_new_status_text = STATUS_MAP.get(new_status_key, {}).get(client_lang, new_status_key) # Локализация статуса для клиента

        # Формируем сводку заказа для клиента (можно использовать ту же логику, что и в confirm_order)
        total = quantity * PRICE_PER_BOTTLE
        display_address = address if address else (TEXT[client_lang].get('location', 'Локация/Joylashuv') if lat is not None else TEXT[client_lang].get('not_specified', 'Не указан/Belgilangan emas'))

        # Получаем имя и контакт клиента для сводки
        client_info_db = {}
        try:
            async with db.execute("SELECT name, contact, username FROM clients WHERE user_id=?", (client_id,)) as cur:
                row = await cur.fetchone()
                if row:
                    client_info_db = {"name": row[0], "contact": row[1], "username": row[2]}
        except Exception as e:
             logger.error(f"Ошибка при получении инфо о клиенте {client_id} для уведомления о статусе: {e}")
             # Если не удалось получить из БД, используем данные из order_row
             client_info_db = {"name": "Не указан", "contact": contact, "username": ""}


        client_summary = (
            f"👤 {client_info_db.get('name', 'Не указан')}" + (f" (@{client_info_db.get('username')})" if client_info_db.get('username') else "") + "\n"
            f"📞 Основной: {client_info_db.get('contact')}\n"
            f"📞 Доп.: {additional_contact or ('–' if client_lang == 'ru' else '–')}\n"
            f"📍 Адрес: {display_address}\n"
            f"🔢 Количество: {quantity} " + ("шт" if client_lang == "ru" else "dona") + f" (Общая сумма: {total:,} " + ("сум" if client_lang == "ru" else "so'm") + ")\n"
        )


        client_notification_text = TEXT[client_lang]['client_status_update'].format(
            order_id=order_id,
            status=client_new_status_text, # Используем локализованный текст для клиента
            order_summary=client_summary
        )
        try:
            await bot.send_message(client_id, client_notification_text)
        except Exception as e:
            logger.error(f"Не удалось отправить уведомление об обновлении статуса заказа {order_id} клиенту {client_id}: {e}")


    except Exception as e:
        logger.error(f"Ошибка при обработке изменения статуса заказа {callback.data} админом {uid}: {e}")
        await callback.answer(TEXT[lang]['error_processing'], show_alert=True)


# --- Основные хэндлеры процесса заказа ---

@dp.message(Command("start"))
async def cmd_start(message: types.Message, state: FSMContext):
    logger.info(f"/start получена от {message.from_user.id} в чате: {message.chat.type}")
    await state.clear() # Всегда очищаем состояние при старте нового процесса
    uid = message.from_user.id

    is_registered = False
    lang = 'ru' # Дефолт язык
    name = None
    contact = None

    try:
        async with db.execute("SELECT name, contact, language FROM clients WHERE user_id=?", (uid,)) as cur:
            row = await cur.fetchone()
            if row:
                db_name, db_contact, db_lang = row
                # Если пользователь есть в БД, используем его сохраненный язык
                lang = db_lang or 'ru'
                await state.update_data(language=lang) # Сохраняем язык в state

                if db_name: # Если имя заполнено - пользователь зарегистрирован
                    name = db_name
                    contact = db_contact
                    await state.update_data(name=name, contact=contact)
                    is_registered = True
                    logger.info(f"Пользователь {uid} зарегистрирован. Язык: {lang}. Имя: {name}") # Логируем имя
                else: # Пользователь выбрал язык, но не завершил регистрацию имени
                     logger.info(f"Пользователь {uid} не завершил регистрацию. Язык: {lang}")
                     # Предлагаем отправить контакт
                     await state.set_state(OrderForm.contact)
                     await message.reply(TEXT[lang]['send_contact'],
                                         reply_markup=ReplyKeyboardMarkup(
                                             keyboard=[[KeyboardButton(text=BTN[lang]['send_contact'], request_contact=True)],
                                                       [KeyboardButton(text=BTN[lang]['cancel'])]],
                                             resize_keyboard=True))
                     return # Выходим из хэндлера

    except Exception as e:
        logger.error(f"Ошибка в cmd_start при проверке пользователя {uid}: {e}")
        # В случае ошибки БД или другой, предлагаем выбрать язык как fallback
        await message.reply(TEXT['ru']['error_processing'], reply_markup=kb_language_select())
        await state.set_state(LangSelect.choosing)
        return

    # Если пользователь зарегистрирован (is_registered == True)
    if is_registered:
        # Отправляем приветствие и предлагаем начать заказ
        greeting_text = TEXT[lang]['greeting_prompt'].format(name=name)
        next_step_text = TEXT[lang]['send_location']
        await message.reply(greeting_text + next_step_text, reply_markup=kb_location(lang))
        await state.set_state(OrderForm.location)
    else:
        # Пользователь новый (row is None)
        logger.info(f"Пользователь {uid} новый. Предлагаем выбрать язык.")
        await message.reply(TEXT['ru']['choose_language'], reply_markup=kb_language_select())
        await state.set_state(LangSelect.choosing)


@dp.message(LangSelect.choosing, F.text.in_(["🇷🇺 Русский", "🇺🇿 Ўзбек"]))
async def process_lang(message: types.Message, state: FSMContext):
    lang = "ru" if message.text.startswith("🇷🇺") else "uz"
    await state.update_data(language=lang)
    uid = message.from_user.id
    usernm = message.from_user.username or ""

    try:
        await db.execute(
            "INSERT INTO clients(user_id, username, language) VALUES(?, ?, ?) "
            "ON CONFLICT(user_id) DO UPDATE SET username=excluded.username, language=excluded.language",
            (uid, usernm, lang)
        )
        await db.commit()
        logger.info(f"Пользователь {uid} выбрал язык: {lang}")
    except Exception as e:
         logger.error(f"Ошибка в process_lang при сохранении клиента {uid}: {e}")
         await message.reply(TEXT[lang]['error_processing'])
         await state.clear()
         await message.reply(TEXT[lang]['process_cancelled'], reply_markup=kb_main(lang, uid in ADMIN_CHAT_IDS, await is_user_registered(uid)))
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
        await db.execute(
            "UPDATE clients SET contact=?, username=? WHERE user_id=?",
            (formatted, usernm, uid))
        await db.commit()
        logger.info(f"Пользователь {uid} сохранил контакт: {formatted}")
    except Exception as e:
         logger.error(f"Ошибка в reg_contact при обновлении клиента {uid}: {e}")
         await message.reply(TEXT[lang]['error_processing'])
         await state.clear()
         await message.reply(TEXT[lang]['process_cancelled'], reply_markup=kb_main(lang, uid in ADMIN_CHAT_IDS, await is_user_registered(uid)))
         return

    await state.update_data(contact=formatted)
    await message.reply(TEXT[lang]['contact_saved'],
                        reply_markup=ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text=BTN[lang]['cancel'])]], resize_keyboard=True))
    await state.set_state(OrderForm.name)

@dp.message(OrderForm.contact) # Ловит любой другой текст/тип контента в этом состоянии
async def prompt_contact_again(message: types.Message, state: FSMContext):
    data = await state.get_data()
    lang = await get_user_lang(message.from_user.id, state)
    # Кнопка "Отменить" обрабатывается отдельным хэндлером handle_cancel_btn
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

    # Кнопка "Отменить" обрабатывается отдельным хэндлером handle_cancel_btn
    # Этот хэндлер сработает только для текста, не совпадающего с кнопкой "Отменить".

    if len(name.split()) < 2:
         return await message.reply(TEXT[lang]['please_full_name'])

    uid = message.from_user.id
    try:
        await db.execute("UPDATE clients SET name=? WHERE user_id=?", (name, uid))
        await db.commit()
        logger.info(f"Пользователь {uid} сохранил имя текстом: {name}")
    except Exception as e:
         logger.error(f"Ошибка в reg_name_text при обновлении клиента {uid}: {e}")
         await message.reply(TEXT[lang]['error_processing'])
         await state.clear()
         await message.reply(TEXT[lang]['process_cancelled'], reply_markup=kb_main(lang, uid in ADMIN_CHAT_IDS, await is_user_registered(uid)))
         return

    await state.update_data(name=name)
    await message.reply(TEXT[lang]['name_saved'].format(name=name), reply_markup=kb_location(lang))
    await state.set_state(OrderForm.location)

@dp.message(OrderForm.name, F.content_type == "photo")
async def reg_name_photo(message: types.Message, state: FSMContext):
    data = await state.get_data()
    lang = await get_user_lang(message.from_user.id, state)
    file_id = message.photo[-1].file_id
    uid = message.from_user.id
    name = f"Фото паспорта: {file_id}"

    try:
        await db.execute("UPDATE clients SET name=? WHERE user_id=?", (name, uid))
        await db.commit()
        logger.info(f"Пользователь {uid} сохранил фото паспорта: {file_id}")
    except Exception as e:
         logger.error(f"Ошибка в reg_name_photo при обновлении клиента {uid}: {e}")
         await message.reply(TEXT[lang]['error_processing'])
         await state.clear()
         await message.reply(TEXT[lang]['process_cancelled'], reply_markup=kb_main(lang, uid in ADMIN_CHAT_IDS, await is_user_registered(uid)))
         return

    await state.update_data(name=name)
    await message.reply(TEXT[lang]['name_saved'].format(name="по фото"), reply_markup=kb_location(lang))
    await state.set_state(OrderForm.location)

@dp.message(OrderForm.location, F.content_type == "location")
async def loc_received(message: types.Message, state: FSMContext):
    data = await state.get_data()
    lang = await get_user_lang(message.from_user.id, state)
    loc = message.location
    await state.update_data(location_lat=loc.latitude, location_lon=loc.longitude, address=None) # Сбрасываем адрес, если прислали локацию
    await message.reply(TEXT[lang]['address_prompt'], reply_markup=kb_cancel_back(lang)) # Просим уточнить адрес (на всякий случай)
    await state.set_state(OrderForm.address)

# Хэндлер для кнопки "Ввести адрес вручную" в состоянии OrderForm.location
@dp.message(OrderForm.location, F.text.in_([BTN['ru']['enter_address'], BTN['uz']['enter_address']]))
async def enter_addr_manual(message: types.Message, state: FSMContext):
    data = await state.get_data()
    lang = await get_user_lang(message.from_user.id, state)
    await state.update_data(location_lat=None, location_lon=None, address=None) # Сбрасываем локацию и адрес
    await message.reply(TEXT[lang]['address_prompt'], reply_markup=kb_cancel_back(lang))
    await state.set_state(OrderForm.address)

# Хэндлер для текста в состоянии OrderForm.location, который не является кнопкой
@dp.message(OrderForm.location, F.text)
async def handle_location_text_input(message: types.Message, state: FSMContext):
    data = await state.get_data()
    lang = await get_user_lang(message.from_user.id, state)
    # Этот хэндлер сработает, если пользователь ввел текст, который не совпадает
    # ни с одной из кнопок на клавиатуре kb_location.
    await message.reply(TEXT[lang]['invalid_input'] + "\n\n" + TEXT[lang]['send_location'], reply_markup=kb_location(lang))


@dp.message(OrderForm.address, F.text) # Ловим любой текст в этом состоянии (кнопки Назад/Отмена ловятся ранее)
async def handle_address_text(message: types.Message, state: FSMContext):
    data = await state.get_data()
    lang = await get_user_lang(message.from_user.id, state)
    addr = message.text.strip()

    if not addr:
        return await message.reply(TEXT[lang]['address_prompt'])

    await state.update_data(address=addr)
    await message.reply(TEXT[lang]['additional_prompt'], reply_markup=kb_additional(lang))
    await state.set_state(OrderForm.additional)


@dp.message(OrderForm.additional, F.text) # Ловим любой текст в этом состоянии (кнопки Пропустить/Назад/Отмена ловятся ранее)
async def handle_additional_text(message: types.Message, state: FSMContext):
    data = await state.get_data()
    lang = await get_user_lang(message.from_user.id, state)
    extra = message.text.strip()

    await state.update_data(additional_contact=extra)

    await message.reply(TEXT[lang]['input_quantity'].format(price=PRICE_PER_BOTTLE), reply_markup=kb_quantity(lang))
    await state.set_state(OrderForm.quantity)


@dp.message(OrderForm.quantity, F.text) # Ловим любой текст в этом состоянии (кнопки Назад/Отмена ловятся ранее)
async def handle_quantity_text(message: types.Message, state: FSMContext):
    data = await state.get_data()
    lang = await get_user_lang(message.from_user.id, state)
    text = message.text.strip()

    if not text.isdigit() or int(text) <= 0:
        err = TEXT[lang]['invalid_input'] + (" Введите положительное число." if lang == "ru" else " Iltimos, musbat raqam kiriting.")
        return await message.reply(err)

    qty = int(text)
    await state.update_data(quantity=qty)

    data = await state.get_data() # Получаем обновленные данные, включая quantity
    total = qty * PRICE_PER_BOTTLE

    uid = message.from_user.id
    user_info_db = {}
    try:
        # Получаем актуальное имя и контакт из БД для сводки
        async with db.execute("SELECT name, contact, username FROM clients WHERE user_id=?", (uid,)) as cur:
            row = await cur.fetchone()
            if row:
                user_info_db = {"name": row[0], "contact": row[1], "username": row[2]}
    except Exception as e:
         logger.error(f"Ошибка при получении инфо о клиенте {uid} для сводки: {e}")
         pass # Продолжаем без данных из БД, используя только state


    display_name = user_info_db.get('name') or data.get('name', 'Не указано' if lang == 'ru' else 'Belgilangan emas')
    username = user_info_db.get('username') or message.from_user.username or ""
    display_name_with_username = f"{display_name} (@{username})" if username else display_name
    contact_display = user_info_db.get('contact') or data.get('contact', 'Не указано' if lang == 'ru' else 'Belgilangan emas')
    additional_contact_display = data.get('additional_contact') or ('–' if lang == 'ru' else '–')
    address_display = data.get('address', 'Не указано' if lang == 'ru' else 'Belgilangan emas')


    summary = (
        f"{TEXT[lang]['order_summary']}\n\n"
        f"👤 {display_name_with_username}\n"
        f"📞 Основной: {contact_display}\n"
        f"📞 Доп.: {additional_contact_display}\n"
        f"📍 Адрес: {address_display}\n"
        f"🔢 Количество: {qty} " + ("шт" if lang == "ru" else "dona") + f" (Общая сумма: {total:,} " + ("сум" if lang == "ru" else "so'm") + ")\n"
    )

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅", callback_data="order_confirm")],
        [InlineKeyboardButton(text="❌", callback_data="order_cancel")]
    ])
    await message.reply(summary, reply_markup=kb)
    await state.set_state(OrderForm.confirm)

# --- Хэндлеры инлайн-кнопок подтверждения заказа ---

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

    if not (contact and (address or (location_lat is not None and location_lon is not None)) and quantity):
         logger.error(f"Не хватает данных для заказа от пользователя {uid}. State: {data}")
         await callback.message.edit_text(TEXT[lang]['error_processing'] + " " + (TEXT[lang]['start_over'] if lang == "ru" else "Yangi boshlash tugmasini bosib qaytadan urinib ko'ring."), reply_markup=None)
         await state.clear()
         await bot.send_message(uid, TEXT[lang]['back_to_main'], reply_markup=kb_main(lang, uid in ADMIN_CHAT_IDS, await is_user_registered(uid)))
         return

    now = datetime.now()
    order_time_str = now.strftime("%Y-%m-%d %H:%M:%S")
    localized_date_str = localize_date(now, lang)

    order_id = None
    try:
        cursor = await db.cursor()
        # Начальный статус 'pending'
        await cursor.execute(
            "INSERT INTO orders(user_id, contact, additional_contact, location_lat, location_lon, address, quantity, order_time, status) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (uid, contact, additional_contact, location_lat, location_lon, address, quantity, order_time_str, 'pending')
        )
        await db.commit()
        order_id = cursor.lastrowid
        await cursor.close()
        logger.info(f"Новый заказ №{order_id} создан пользователем {uid}")

    except Exception as e:
        logger.error(f"Ошибка при сохранении заказа в БД для пользователя {uid}: {e}")
        await callback.message.edit_text(TEXT[lang]['error_processing'] + " " + (TEXT[lang]['back_to_main'] if lang == "ru" else "Bosh menyuga qaytish."), reply_markup=None)
        await state.clear()
        await bot.send_message(uid, TEXT[lang]['back_to_main'], reply_markup=kb_main(lang, uid in ADMIN_CHAT_IDS, await is_user_registered(uid)))
        return

    # Получаем актуальное имя и username из БД для уведомления админа
    user_info_db = {}
    try:
        async with db.execute("SELECT name, username FROM clients WHERE user_id=?", (uid,)) as cur:
            row = await cur.fetchone()
            if row:
                user_info_db = {"name": row[0], "username": row[1]}
    except Exception as e:
         logger.error(f"Ошибка при получении инфо о клиенте {uid} для админ уведомления: {e}")
         # Если не удалось получить из БД, используем данные из state (они должны быть)
         user_info_db = {"name": data.get('name'), "username": callback.from_user.username}


    full_name = user_info_db.get('name', 'Не указан' if lang == 'ru' else 'Belgilangan emas')
    uname = user_info_db.get('username', "")
    display_name = f"{full_name} (@{uname})" if uname else full_name
    contact_display = user_info_db.get('contact') or data.get('contact', 'Не указано' if lang == 'ru' else 'Belgilangan emas')
    additional_contact_display = data.get('additional_contact') or ('–' if lang == 'ru' else '–')
    address_display = data.get('address', 'Не указано' if lang == 'ru' else 'Belgilangan emas')

    total = quantity * PRICE_PER_BOTTLE

    msg_to_admin = (
        f"📣 <b>Новый заказ</b> (№{order_id})\n\n"
        f"👤 {display_name}\n"
        f"📞 Основной: {contact_display}\n"
        f"📞 Доп.: {additional_contact_display}\n"
        f"📍 Адрес: {address_display}\n"
        f"🔢 Количество: {quantity} шт (Общая сумма: {total:,} сум)\n"
        f"🕒 Время заказа: {localized_date_str}\n"
        f"🆔 User ID: <code>{uid}</code>\n"
        f"✨ Статус: {STATUS_MAP['pending']['ru']}" # Начальный статус для админа всегда на русском в уведомлении
    )

    # Уведомление админов и группы с инлайн-кнопками для статуса (на русском)
    admin_order_kb = kb_admin_order_status(order_id, 'ru') # Кнопки статуса для админа всегда на русском
    all_recipients = set(ADMIN_CHAT_IDS + [GROUP_CHAT_ID])
    for chat_id in all_recipients:
        if chat_id is None: continue # Пропускаем None ID
        try:
            sent_msg = await bot.send_message(chat_id, msg_to_admin, parse_mode=ParseMode.HTML, reply_markup=admin_order_kb)
            # Возможно, сохранять chat_id сообщения админа для будущих обновлений?
            # Например: await db.execute("UPDATE orders SET admin_message_chat_id=?, admin_message_id=? WHERE order_id=?", (sent_msg.chat.id, sent_msg.message_id, order_id))
            if location_lat is not None and location_lon is not None:
                await bot.send_location(chat_id, location_lat, location_lon)
        except Exception as e:
            logger.error(f"Не удалось отправить уведомление о заказе {order_id} в чат {chat_id}: {e}")

    # Редактируем сообщение пользователя
    try:
        await callback.message.edit_reply_markup(reply_markup=None)
        await callback.message.edit_text(callback.message.text + "\n\n" + TEXT[lang]['order_confirmed'], reply_markup=None)
    except Exception as e:
         logger.warning(f"Не удалось отредактировать сообщение после подтверждения заказа {order_id} для пользователя {uid}: {e}")
         await bot.send_message(uid, TEXT[lang]['order_confirmed'], reply_markup=None)

    is_registered = await is_user_registered(uid)
    await bot.send_message(uid, TEXT[lang]['back_to_main'], reply_markup=kb_main(lang, uid in ADMIN_CHAT_IDS, is_registered))
    await state.clear()


@dp.callback_query(StateFilter(OrderForm.confirm), F.data == "order_cancel")
async def cancel_order_callback(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    lang = await get_user_lang(callback.from_user.id, state)
    await callback.answer("❌ Отменено" if lang == "ru" else "❌ Bekor qilindi")

    try:
        await callback.message.edit_reply_markup(reply_markup=None)
        await callback.message.edit_text(callback.message.text + "\n\n" + TEXT[lang]['order_cancelled'], reply_markup=None)
    except Exception as e:
        logger.warning(f"Не удалось отредактировать сообщение после отмены заказа для пользователя {callback.from_user.id}: {e}")
        await bot.send_message(callback.from_user.id, TEXT[lang]['order_cancelled'], reply_markup=None)

    uid = callback.from_user.id
    is_registered = await is_user_registered(uid)
    await bot.send_message(uid, TEXT[lang]['back_to_main'], reply_markup=kb_main(lang, uid in ADMIN_CHAT_IDS, is_registered))
    await state.clear()

# --- Вспомогательная функция для отмены процесса ---
async def cancel_process(message: types.Message, state: FSMContext):
    uid = message.from_user.id
    lang = await get_user_lang(uid, state)

    is_registered = await is_user_registered(uid)

    await state.clear()
    await message.reply(TEXT[lang]['process_cancelled'], reply_markup=kb_main(lang, uid in ADMIN_CHAT_IDS, is_registered))


# --- Default handler (ловит все остальные текстовые сообщения) ---
# Этот хэндлер должен быть зарегистрирован ПОСЛЕДНИМ
@dp.message(F.text)
async def default_text_handler(message: types.Message, state: FSMContext):
    uid = message.from_user.id
    lang = await get_user_lang(uid, state)

    is_registered = await is_user_registered(uid)

    await message.reply(TEXT[lang]['invalid_input'] + " " + TEXT[lang]['back_to_main'], reply_markup=kb_main(lang, uid in ADMIN_CHAT_IDS, is_registered))
    await state.clear()


# Если пользователь отправляет что-то, что не текст (стикеры, аудио, видео и т.п.)
@dp.message(~F.text & ~F.content_type.in_([ContentType.CONTACT, ContentType.LOCATION, ContentType.PHOTO]))
async def default_other_handler(message: types.Message, state: FSMContext):
    uid = message.from_user.id
    lang = await get_user_lang(uid, state)

    is_registered = await is_user_registered(uid)

    await message.reply(TEXT[lang]['invalid_input'] + " " + TEXT[lang]['back_to_main'], reply_markup=kb_main(lang, uid in ADMIN_CHAT_IDS, is_registered))
    await state.clear()


# --- Инициализация БД ---
async def init_db():
    """Инициализирует базу данных (создает таблицы, если их нет)."""
    logger.info("Инициализация базы данных...")
    if db is None:
         logger.error("Соединение с БД не установлено в init_db.")
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
                status TEXT, -- Используем короткий ключ: 'pending', 'accepted', 'in_progress', 'completed', 'rejected'
                FOREIGN KEY (user_id) REFERENCES clients (user_id) ON DELETE CASCADE
            )
        ''')
        await db.commit()
        logger.info("База данных инициализирована.")
    except Exception as e:
         logger.error(f"Ошибка при инициализации БД: {e}")


async def main():
    global db
    logger.info("Подключение к базе данных...")
    try:
        db = await aiosqlite.connect("clients.db")
        db.row_factory = aiosqlite.Row
        logger.info("Подключение к БД успешно.")
        await init_db()
    except Exception as e:
        logger.critical(f"Критическая ошибка при подключении или инициализации БД: {e}")
        return

    logger.info("Запуск бота...")
    try:
        await bot.delete_webhook(drop_pending_updates=True)
    except Exception as e:
        logger.warning(f"Не удалось удалить вебхук или ожидающие обновления: {e}")

    try:
        # Порядок регистрации хэндлеров:
        # 1. Команды (/start)
        dp.message.register(cmd_start, Command("start"))

        # 2. Хэндлеры кнопок, специфичные для состояний FSM (Отмена, Назад, Пропустить)
        dp.message.register(handle_cancel_btn, StateFilter(OrderForm), F.text.in_([BTN['ru']['cancel'], BTN['uz']['cancel']]))
        dp.message.register(handle_back_btn, StateFilter(OrderForm.address, OrderForm.additional, OrderForm.quantity), F.text.in_([BTN['ru']['back'], BTN['uz']['back']]))
        dp.message.register(handle_skip_btn, OrderForm.additional, F.text.in_([BTN['ru']['skip'], BTN['uz']['skip']]))

        # 3. Хэндлеры FSM по типам контента (contact, location, photo, text) для конкретных состояний
        dp.message.register(process_lang, LangSelect.choosing, F.text.in_(["🇷🇺 Русский", "🇺🇿 Ўзбек"]))
        dp.message.register(reg_contact, OrderForm.contact, F.content_type == "contact")
        dp.message.register(prompt_contact_again, OrderForm.contact) # Ловит все остальное в этом состоянии
        dp.message.register(reg_name_text, OrderForm.name, F.content_type == "text")
        dp.message.register(reg_name_photo, OrderForm.name, F.content_type == "photo")
        dp.message.register(loc_received, OrderForm.location, F.content_type == "location")
        dp.message.register(enter_addr_manual, OrderForm.location, F.text.in_([BTN['ru']['enter_address'], BTN['uz']['enter_address']]))
        dp.message.register(handle_location_text_input, OrderForm.location, F.text) # Текст в location, который не кнопка
        dp.message.register(handle_address_text, OrderForm.address, F.text)
        dp.message.register(handle_additional_text, OrderForm.additional, F.text)
        dp.message.register(handle_quantity_text, OrderForm.quantity, F.text)

        # 4. Хэндлеры инлайн-кнопок (подтверждение/отмена заказа, изменение статуса админом, админские подтверждения)
        dp.callback_query.register(confirm_order, StateFilter(OrderForm.confirm), F.data == "order_confirm")
        dp.callback_query.register(cancel_order_callback, StateFilter(OrderForm.confirm), F.data == "order_cancel")
        dp.callback_query.register(handle_admin_clear_callback, AdminStates.main, F.data.startswith("admin_clear_"))
        dp.callback_query.register(handle_confirm_clear_clients, AdminStates.confirm_clear_clients, F.data.startswith("admin_confirm_clients_"))
        dp.callback_query.register(handle_confirm_clear_orders, AdminStates.confirm_clear_orders, F.data.startswith("admin_confirm_orders_"))
        dp.callback_query.register(handle_admin_set_status, F.data.startswith("set_status:")) # Хэндлер изменения статуса


        # 5. Общие хэндлеры кнопок (Мои заказы, Сменить язык, Начать сначала, Управление БД), работающие из любого состояния
        dp.message.register(handle_start_over_btn, F.text.in_([BTN['ru']['start_over'], BTN['uz']['start_over']]))
        dp.message.register(handle_change_lang_btn, F.text.in_([TEXT['ru']['change_lang'], TEXT['uz']['change_lang']]))
        dp.message.register(handle_my_orders_btn, F.text.in_([BTN['ru']['my_orders'], BTN['uz']['my_orders']]))
        dp.message.register(handle_edit_order_btn, F.text.in_([BTN['ru']['edit_order'], BTN['uz']['edit_order']])) # Placeholder
        dp.message.register(handle_manage_db_btn, F.text.in_([BTN['ru']['manage_db'], BTN['uz']['manage_db']])) # Admin only


        # 6. Дефолтные хэндлеры (ловящие все остальное), размещенные в конце.
        # Non-text first
        dp.message.register(default_other_handler, ~F.text & ~F.content_type.in_([ContentType.CONTACT, ContentType.LOCATION, ContentType.PHOTO]))
        # Text last
        dp.message.register(default_text_handler, F.text)


        await dp.start_polling(bot)
    except Exception as e:
        logger.error(f"Ошибка во время работы поллинга: {e}")
    finally:
        if db:
            await db.close()
            logger.info("Соединение с БД закрыто.")
        logger.info("Бот остановлен.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Бот остановлен вручную по KeyboardInterrupt")
    except Exception as e:
        logger.critical(f"Бот завершился с необработанным исключением: {e}", exc_info=True)