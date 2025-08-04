import os
import logging
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup
from telegram.ext import (
    ApplicationBuilder, CommandHandler, MessageHandler,
    CallbackQueryHandler, ContextTypes, filters
)
from datetime import datetime, time, timedelta
from tabulate import tabulate
import pytz
import re
import calendar
import asyncio

# Import database functions
from database import (
    init_db, save_user_bet, get_user_bets, delete_user_bet,
    save_break_limit, get_break_limit,
    save_power_number, get_power_number,
    save_user_com_za, get_user_com_za, get_all_users,
    get_available_dates, delete_date_data
)

# Environment variables
TOKEN = os.getenv("BOT_TOKEN")

# Logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Timezone setup
MYANMAR_TIMEZONE = pytz.timezone('Asia/Yangon')

# Globals (for non-persistent data)
admin_id = None
date_control = {}  # {date_key: True/False}
overbuy_list = {}  # {date_key: {username: {num: amount}}}
message_store = {}  # {(user_id, message_id): (sent_message_id, bets, total_amount, date_key)}
overbuy_selections = {}  # {date_key: {username: {num: amount}}}
current_working_date = None  # For admin date selection
closed_numbers = set()  # Store closed numbers

def reverse_number(n):
    s = str(n).zfill(2)
    return int(s[::-1])

def get_time_segment():
    now = datetime.now(MYANMAR_TIMEZONE).time()
    return "AM" if now < time(12, 0) else "PM"

def get_current_date_key():
    now = datetime.now(MYANMAR_TIMEZONE)
    return f"{now.strftime('%d/%m/%Y')} {get_time_segment()}"

async def show_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = []
    if update.effective_user.id == admin_id:
        keyboard = [
            ["အရောင်းဖွင့်ရန်", "အရောင်းပိတ်ရန်"],
            ["လည်ချာ", "ဘရိတ်သတ်မှတ်ရန်"],
            ["လျှံဂဏန်းများဝယ်ရန်", "ပေါက်သီးထည့်ရန်"],
            ["ကော်နှင့်အဆ သတ်မှတ်ရန်", "လက်ရှိအချိန်မှစုစုပေါင်း"],
            ["ဂဏန်းနှင့်ငွေပေါင်း", "ကော်မရှင်များ"],
            ["ရက်ချိန်းရန်", "တစ်ယောက်ခြင်းစာရင်း"],
            ["ဟော့ဂဏန်းပိတ်ရန်", "ရက်အလိုက်စာရင်းစုစုပေါင်း"],
            ["ရက်အကုန်ဖျက်ရန်", "ရက်အလိုက်ဖျက်ရန်"]
        ]
    else:
        keyboard = [
            ["တစ်ယောက်ခြင်းစာရင်း"]
        ]
    
    reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
    await update.message.reply_text("မီနူးကိုရွေးချယ်ပါ", reply_markup=reply_markup)

async def handle_menu_selection(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text
    command_map = {
        "အရောင်းဖွင့်ရန်": "/dateopen",
        "အရောင်းပိတ်ရန်": "/dateclose",
        "လည်ချာ": "/ledger",
        "ဘရိတ်သတ်မှတ်ရန်": "/break",
        "လျှံဂဏန်းများဝယ်ရန်": "/overbuy",
        "ပေါက်သီးထည့်ရန်": "/pnumber",
        "ကော်နှင့်အဆ သတ်မှတ်ရန်": "/comandza",
        "လက်ရှိအချိန်မှစုစုပေါင်း": "/total",
        "ဂဏန်းနှင့်ငွေပေါင်း": "/tsent",
        "ကော်မရှင်များ": "/alldata",
        "ရက်အကုန်ဖျက်ရန်": "/reset",
        "တစ်ယောက်ခြင်းစာရင်း": "/posthis",
        "ရက်အလိုက်စာရင်းစုစုပေါင်း": "/dateall",
        "ရက်ချိန်းရန်": "/Cdate",
        "ရက်အလိုက်ဖျက်ရန်": "/Ddate",
        "ဟော့ဂဏန်းပိတ်ရန်": "/numclose"
    }
    
    if text in command_map:
        command = command_map[text]
        if command == "/dateopen":
            await dateopen(update, context)
        elif command == "/dateclose":
            await dateclose(update, context)
        elif command == "/ledger":
            await ledger_summary(update, context)
        elif command == "/break":
            await break_command(update, context)
        elif command == "/overbuy":
            await overbuy(update, context)
        elif command == "/pnumber":
            await pnumber(update, context)
        elif command == "/comandza":
            await comandza(update, context)
        elif command == "/total":
            await total(update, context)
        elif command == "/tsent":
            await tsent(update, context)
        elif command == "/alldata":
            await alldata(update, context)
        elif command == "/reset":
            await reset_data(update, context)
        elif command == "/posthis":
            await posthis(update, context)
        elif command == "/dateall":
            await dateall(update, context)
        elif command == "/Cdate":
            await change_working_date(update, context)
        elif command == "/Ddate":
            await delete_date(update, context)
        elif command == "/numclose":
            await numclose(update, context)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global admin_id, current_working_date
    
    # Initialize database
    try:
        await init_db()
        logger.info("Database initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize database: {str(e)}")
        await update.message.reply_text("❌ Database initialization failed. Please check logs.")
        return
    
    admin_id = update.effective_user.id
    current_working_date = get_current_date_key()
    logger.info(f"Admin set to: {admin_id}")
    await update.message.reply_text("🤖 Bot started. Admin privileges granted!")
    await show_menu(update, context)

async def dateopen(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global admin_id
    if update.effective_user.id != admin_id:
        await update.message.reply_text("❌ Admin only command")
        return
        
    key = get_current_date_key()
    date_control[key] = True
    logger.info(f"Ledger opened for {key}")
    await update.message.reply_text(f"✅ {key} စာရင်းဖွင့်ပြီးပါပြီ")

async def dateclose(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global admin_id
    if update.effective_user.id != admin_id:
        await update.message.reply_text("❌ Admin only command")
        return
        
    key = get_current_date_key()
    date_control[key] = False
    logger.info(f"Ledger closed for {key}")
    await update.message.reply_text(f"✅ {key} စာရင်းပိတ်လိုက်ပါပြီ")

async def numclose(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global admin_id, closed_numbers
    if update.effective_user.id != admin_id:
        await update.message.reply_text("❌ Admin only command")
        return

    if not context.args:
        if closed_numbers:
            nums_str = " ".join(f"{n:02d}" for n in sorted(closed_numbers))
            keyboard = [[InlineKeyboardButton("🗑 Delete All", callback_data="numclose_delete_all")]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await update.message.reply_text(
                f"🔒 Closed Numbers: {nums_str}",
                reply_markup=reply_markup
            )
        else:
            await update.message.reply_text("ℹ️ Usage: /numclose [numbers]\nℹ️ No numbers currently closed")
        return

    try:
        text = " ".join(context.args)
        new_numbers = set()
        
        # Check for special cases
        special_cases = {
            "အပူး": [0, 11, 22, 33, 44, 55, 66, 77, 88, 99],
            "ပါဝါ": [5, 16, 27, 38, 49, 50, 61, 72, 83, 94],
            "နက္ခ": [7, 18, 24, 35, 42, 53, 69, 70, 81, 96],
            "ညီကို": [1, 12, 23, 34, 45, 56, 67, 78, 89, 90],
            "ကိုညီ": [9, 10, 21, 32, 43, 54, 65, 76, 87, 98],
        }

        dynamic_types = ["ထိပ်", "ပိတ်", "ဘရိတ်", "အပါ"]
        
        found_special = False
        for case_name, case_numbers in special_cases.items():
            if case_name in text:
                new_numbers.update(case_numbers)
                found_special = True
                break

        if not found_special:
            for dtype in dynamic_types:
                if dtype in text:
                    parts = re.findall(r'\d+', text)
                    if parts:
                        digit = int(parts[0])
                        if dtype == "ထိပ်":
                            new_numbers.update([digit * 10 + j for j in range(10)])
                        elif dtype == "ပိတ်":
                            new_numbers.update([j * 10 + digit for j in range(10)])
                        elif dtype == "ဘရိတ်":
                            new_numbers.update([n for n in range(100) if (n//10 + n%10) % 10 == digit])
                        elif dtype == "အပါ":
                            tens = [digit * 10 + j for j in range(10)]
                            units = [j * 10 + digit for j in range(10)]
                            new_numbers.update(tens + units)
                    found_special = True
                    break

        if not found_special:
            numbers = re.findall(r'\d+', text)
            for num in numbers:
                num_int = int(num)
                if 0 <= num_int <= 99:
                    new_numbers.add(num_int)
                if 'r' in text.lower():
                    new_numbers.add(reverse_number(num_int))

        closed_numbers.update(new_numbers)
        
        nums_str = " ".join(f"{n:02d}" for n in sorted(closed_numbers))
        keyboard = [[InlineKeyboardButton("🗑 Delete All", callback_data="numclose_delete_all")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await update.message.reply_text(
            f"✅ Closed numbers updated:\n🔒 {nums_str}",
            reply_markup=reply_markup
        )

    except Exception as e:
        logger.error(f"Error in numclose: {str(e)}")
        await update.message.reply_text("❌ Error processing numbers. Please check your input.")

async def numclose_delete_all(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    global closed_numbers
    closed_numbers = set()
    await query.edit_message_text("✅ All closed numbers have been cleared")

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        user = update.effective_user
        text = update.message.text
        
        if not user or not user.username:
            await update.message.reply_text("❌ ကျေးဇူးပြု၍ Telegram username သတ်မှတ်ပါ")
            return

        # Check if admin is posting for another user
        target_username = None
        if user.id == admin_id and text.startswith('@'):
            lines = text.split('\n')
            if len(lines) > 1:
                possible_username = lines[0].strip()[1:]  # Remove @
                if possible_username in await get_all_users():  # Check if valid username
                    target_username = possible_username
                    text = '\n'.join(lines[1:])  # Remove first line (@username)
                else:
                    await update.message.reply_text(f"❌ User @{possible_username} မရှိပါ")
                    return

        # Use target_username if exists, otherwise use sender's username
        username = target_username if target_username else user.username

        key = get_current_date_key()
        if not date_control.get(key, False):
            await update.message.reply_text("❌ စာရင်းပိတ်ထားပါသည်")
            return

        if not text:
            await update.message.reply_text("⚠️ မက်ဆေ့ဂျ်မရှိပါ")
            return

        lines = text.split('\n')
        all_bets = []
        total_amount = 0
        blocked_bets = []

        for line in lines:
            line = line.strip()
            if not line:
                continue

            # Check for wheel cases
            if 'အခွေ' in line or 'အပူးပါအခွေ' in line:
                if 'အခွေ' in line:
                    parts = line.split('အခွေ')
                    base_part = parts[0]
                    amount_part = parts[1]
                else:
                    parts = line.split('အပူးပါအခွေ')
                    base_part = parts[0]
                    amount_part = parts[1]
                
                base_numbers = ''.join([c for c in base_part if c.isdigit()])
                amount = int(''.join([c for c in amount_part if c.isdigit()]))
                
                pairs = []
                for i in range(len(base_numbers)):
                    for j in range(len(base_numbers)):
                        if i != j:
                            num = int(base_numbers[i] + base_numbers[j])
                            if num not in pairs:
                                pairs.append(num)
                
                if 'အပူးပါအခွေ' in line:
                    for d in base_numbers:
                        double = int(d + d)
                        if double not in pairs:
                            pairs.append(double)
                
                for num in pairs:
                    if num in closed_numbers:
                        blocked_bets.append(f"{num:02d}-{amount}")
                    else:
                        all_bets.append(f"{num:02d}-{amount}")
                        total_amount += amount
                continue

            # Check for special cases
            special_cases = {
                "အပူး": [0, 11, 22, 33, 44, 55, 66, 77, 88, 99],
                "ပါဝါ": [5, 16, 27, 38, 49, 50, 61, 72, 83, 94],
                "နက္ခ": [7, 18, 24, 35, 42, 53, 69, 70, 81, 96],
                "ညီကို": [1, 12, 23, 34, 45, 56, 67, 78, 89, 90],
                "ကိုညီ": [9, 10, 21, 32, 43, 54, 65, 76, 87, 98],
            }

            dynamic_types = ["ထိပ်", "ပိတ်", "ဘရိတ်", "အပါ"]
            
            found_special = False
            for case_name, case_numbers in special_cases.items():
                case_variations = [case_name]
                if case_name == "နက္ခ":
                    case_variations.extend(["နခ", "နက်ခ", "နတ်ခ", "နခက်", "နတ်ခက်", "နက်ခက်", "နတ်ခတ်", "နက်ခတ်", "နခတ်", "နခပ်"])
                
                for variation in case_variations:
                    if line.startswith(variation):
                        amount_str = line[len(variation):].strip()
                        amount_str = ''.join([c for c in amount_str if c.isdigit()])
                        
                        if amount_str and int(amount_str) >= 100:
                            amt = int(amount_str)
                            for num in case_numbers:
                                if num in closed_numbers:
                                    blocked_bets.append(f"{num:02d}-{amt}")
                                else:
                                    all_bets.append(f"{num:02d}-{amt}")
                                    total_amount += amt
                            found_special = True
                            break
                    if found_special:
                        break
                if found_special:
                    break
            
            if found_special:
                continue

            for dtype in dynamic_types:
                if dtype in line:
                    numbers = []
                    amount = 0
                    
                    parts = re.findall(r'\d+', line)
                    if parts:
                        amount = int(parts[-1]) if int(parts[-1]) >= 100 else 0
                        digits = [int(p) for p in parts[:-1] if len(p) == 1 and p.isdigit()]
                    
                    if amount >= 100 and digits:
                        numbers = []
                        if dtype == "ထိပ်":
                            for d in digits:
                                numbers.extend([d * 10 + j for j in range(10)])
                        elif dtype == "ပိတ်":
                            for d in digits:
                                numbers.extend([j * 10 + d for j in range(10)])
                        elif dtype == "ဘရိတ်":
                            for d in digits:
                                numbers.extend([n for n in range(100) if (n//10 + n%10) % 10 == d])
                        elif dtype == "အပါ":
                            for d in digits:
                                tens = [d * 10 + j for j in range(10)]
                                units = [j * 10 + d for j in range(10)]
                                numbers.extend(list(set(tens + units)))
                        
                        for num in numbers:
                            if num in closed_numbers:
                                blocked_bets.append(f"{num:02d}-{amount}")
                            else:
                                all_bets.append(f"{num:02d}-{amount}")
                                total_amount += amount
                        found_special = True
                        break
            
            if found_special:
                continue

            if 'r' in line.lower():
                r_pos = line.lower().find('r')
                before_r = line[:r_pos]
                after_r = line[r_pos+1:]
                
                nums_before = re.findall(r'\d+', before_r)
                nums_before = [int(n) for n in nums_before if 0 <= int(n) <= 99]
                
                amounts = re.findall(r'\d+', after_r)
                amounts = [int(a) for a in amounts if int(a) >= 100]
                
                if nums_before and amounts:
                    if len(amounts) == 1:
                        for num in nums_before:
                            if num in closed_numbers:
                                blocked_bets.append(f"{num:02d}-{amounts[0]}")
                            else:
                                all_bets.append(f"{num:02d}-{amounts[0]}")
                                total_amount += amounts[0]
                            
                            rev_num = reverse_number(num)
                            if rev_num in closed_numbers:
                                blocked_bets.append(f"{rev_num:02d}-{amounts[0]}")
                            else:
                                all_bets.append(f"{rev_num:02d}-{amounts[0]}")
                                total_amount += amounts[0]
                    else:
                        for num in nums_before:
                            if num in closed_numbers:
                                blocked_bets.append(f"{num:02d}-{amounts[0]}")
                            else:
                                all_bets.append(f"{num:02d}-{amounts[0]}")
                                total_amount += amounts[0]
                            
                            rev_num = reverse_number(num)
                            if rev_num in closed_numbers:
                                blocked_bets.append(f"{rev_num:02d}-{amounts[1]}")
                            else:
                                all_bets.append(f"{rev_num:02d}-{amounts[1]}")
                                total_amount += amounts[1]
                    continue

            numbers = []
            amount = 0
            
            all_numbers = re.findall(r'\d+', line)
            if all_numbers:
                if int(all_numbers[-1]) >= 100:
                    amount = int(all_numbers[-1])
                    numbers = [int(n) for n in all_numbers[:-1] if 0 <= int(n) <= 99]
                else:
                    for i in range(len(all_numbers)-1):
                        if 0 <= int(all_numbers[i]) <= 99 and int(all_numbers[i+1]) >= 100:
                            numbers.append(int(all_numbers[i]))
                            amount = int(all_numbers[i+1])
                            break
            
            if amount >= 100 and numbers:
                for num in numbers:
                    if num in closed_numbers:
                        blocked_bets.append(f"{num:02d}-{amount}")
                    else:
                        all_bets.append(f"{num:02d}-{amount}")
                        total_amount += amount

        if not all_bets and not blocked_bets:
            await update.message.reply_text("⚠️ အချက်အလက်များကိုစစ်ဆေးပါ\nဥပမာ: 12-1000,12/34-1000 \n 12r1000,12r1000-500")
            return

        # Save all bets to database
        for bet in all_bets:
            num, amt = bet.split('-')
            num = int(num)
            amt = int(amt)
            
            await save_user_bet(username, key, num, amt)

        response_parts = []
        if all_bets:
            response_parts.append("\n".join(all_bets))
            response_parts.append(f"စုစုပေါင်း {total_amount} ကျပ်")
        
        if blocked_bets:
            blocked_nums = ", ".join(set(bet.split('-')[0] for bet in blocked_bets))
            response_parts.append(f"\n🚫 ပိတ်ထားသောဂဏန်းများ: {blocked_nums} (မရပါ)")

        keyboard = [[InlineKeyboardButton("🗑 Delete", callback_data=f"delete:{user.id}:{update.message.message_id}:{key}:{username}")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        sent_message = await update.message.reply_text(
            "\n".join(response_parts),
            reply_markup=reply_markup
        )
        
        message_store[(user.id, update.message.message_id)] = (sent_message.message_id, all_bets, total_amount, key, username)
            
    except Exception as e:
        logger.error(f"Error in handle_message: {str(e)}")
        await update.message.reply_text(f"❌ Error: {str(e)}")
        
        
async def delete_bet(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    try:
        _, user_id_str, message_id_str, date_key, username = query.data.split(':')
        user_id = int(user_id_str)
        message_id = int(message_id_str)
        
        # Only admin can interact with delete button
        if query.from_user.id != admin_id:
            await query.edit_message_text("❌ Admin only action")
            return
            
        keyboard = [
            [InlineKeyboardButton("✅ OK", callback_data=f"confirm_delete:{user_id}:{message_id}:{date_key}:{username}")],
            [InlineKeyboardButton("❌ Cancel", callback_data=f"cancel_delete:{user_id}:{message_id}:{date_key}:{username}")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text("⚠️ သေချာလား? ဒီလောင်းကြေးကိုဖျက်မှာလား?", reply_markup=reply_markup)
        
    except Exception as e:
        logger.error(f"Error in delete_bet: {str(e)}")
        await query.edit_message_text("❌ Error occurred while processing deletion")

async def confirm_delete(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    try:
        _, user_id_str, message_id_str, date_key, username = query.data.split(':')
        user_id = int(user_id_str)
        message_id = int(message_id_str)
        
        if (user_id, message_id) not in message_store:
            await query.edit_message_text("❌ ဒေတာမတွေ့ပါ")
            return
            
        sent_message_id, bets, total_amount, _, _ = message_store[(user_id, message_id)]
        
        # Delete each bet from database
        for bet in bets:
            num, amt = bet.split('-')
            num = int(num)
            amt = int(amt)
            
            await delete_user_bet(username, date_key, num, amt)
        
        del message_store[(user_id, message_id)]
        await query.edit_message_text("✅ လောင်းကြေးဖျက်ပြီးပါပြီ")
        
    except Exception as e:
        logger.error(f"Error in confirm_delete: {str(e)}")
        await query.edit_message_text("❌ Error occurred while deleting bet")

async def cancel_delete(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    try:
        _, user_id_str, message_id_str, date_key, username = query.data.split(':')
        user_id = int(user_id_str)
        message_id = int(message_id_str)
        
        if (user_id, message_id) in message_store:
            sent_message_id, bets, total_amount, _, _ = message_store[(user_id, message_id)]
            response = "\n".join(bets) + f"\nစုစုပေါင်း {total_amount} ကျပ်"
            keyboard = [[InlineKeyboardButton("🗑 Delete", callback_data=f"delete:{user_id}:{message_id}:{date_key}:{username}")]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await query.edit_message_text(response, reply_markup=reply_markup)
        else:
            await query.edit_message_text("ℹ️ ဖျက်ခြင်းကိုပယ်ဖျက်လိုက်ပါပြီ")
            
    except Exception as e:
        logger.error(f"Error in cancel_delete: {str(e)}")
        await query.edit_message_text("❌ Error occurred while canceling deletion")

async def ledger_summary(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global admin_id, current_working_date, closed_numbers
    try:
        if update.effective_user.id != admin_id:
            await update.message.reply_text("❌ Admin only command")
            return
            
        date_key = current_working_date if current_working_date else get_current_date_key()
        
        # Get all bets for this date from database
        bets = await get_user_bets(date_key=date_key)
        
        if not bets:
            await update.message.reply_text(f"ℹ️ {date_key} အတွက် လက်ရှိတွင် လောင်းကြေးမရှိပါ")
            return
            
        # Calculate totals per number
        number_totals = {}
        for bet in bets:
            num = bet['number']
            amt = bet['amount']
            number_totals[num] = number_totals.get(num, 0) + amt
        
        lines = [f"📒 {date_key} လက်ကျန်ငွေစာရင်း"]
        total_all_numbers = 0
        
        for i in range(100):
            total = number_totals.get(i, 0)
            if total > 0:
                pnum = await get_power_number(date_key)
                if pnum is not None and i == pnum:
                    lines.append(f"🔴 {i:02d} ➤ {total} 🔴")
                elif i in closed_numbers:
                    lines.append(f"🚫 {i:02d} ➤ {total} (Closed)")
                else:
                    lines.append(f"{i:02d} ➤ {total}")
                total_all_numbers += total

        if len(lines) == 1:
            await update.message.reply_text(f"ℹ️ {date_key} အတွက် လက်ရှိတွင် လောင်းကြေးမရှိပါ")
        else:
            pnum = await get_power_number(date_key)
            if pnum is not None:
                lines.append(f"\n🔴 Power Number: {pnum:02d} ➤ {number_totals.get(pnum, 0)}")
            
            if closed_numbers:
                closed_str = " ".join(f"{n:02d}" for n in sorted(closed_numbers))
                lines.append(f"\n🔒 Closed Numbers: {closed_str}")
            
            lines.append(f"\n💰 စုစုပေါင်း: {total_all_numbers} ကျပ်")
            await update.message.reply_text("\n".join(lines))
    except Exception as e:
        logger.error(f"Error in ledger: {str(e)}")
        await update.message.reply_text(f"❌ Error: {str(e)}")

        
async def break_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global admin_id, current_working_date
    try:
        if update.effective_user.id != admin_id:
            await update.message.reply_text("❌ Admin only command")
            return
            
        # Determine which date to work on
        date_key = current_working_date if current_working_date else get_current_date_key()
            
        if not context.args:
            limit = await get_break_limit(date_key)
            if limit is not None:
                await update.message.reply_text(f"ℹ️ Usage: /break [limit]\nℹ️ လက်ရှိတွင် break limit: {limit}")
            else:
                await update.message.reply_text(f"ℹ️ Usage: /break [limit]\nℹ️ {date_key} အတွက် break limit မသတ်မှတ်ရသေးပါ")
            return
            
        try:
            new_limit = int(context.args[0])
            await save_break_limit(date_key, new_limit)
            await update.message.reply_text(f"✅ {date_key} အတွက် Break limit ကို {new_limit} အဖြစ်သတ်မှတ်ပြီးပါပြီ")
            
            # Get all bets for this date to show over-limit numbers
            bets = await get_user_bets(date_key=date_key)
            if not bets:
                await update.message.reply_text(f"ℹ️ {date_key} အတွက် လောင်းကြေးမရှိသေးပါ")
                return
                
            # Calculate totals per number
            number_totals = {}
            for bet in bets:
                num = bet['number']
                amt = bet['amount']
                number_totals[num] = number_totals.get(num, 0) + amt
            
            msg = [f"📌 {date_key} အတွက် Limit ({new_limit}) ကျော်ဂဏန်းများ:"]
            found = False
            
            for num, amt in number_totals.items():
                if amt > new_limit:
                    msg.append(f"{num:02d} ➤ {amt - new_limit}")
                    found = True
            
            if not found:
                await update.message.reply_text(f"ℹ️ {date_key} အတွက် ဘယ်ဂဏန်းမှ limit ({new_limit}) မကျော်ပါ")
            else:
                await update.message.reply_text("\n".join(msg))
                
        except ValueError:
            await update.message.reply_text("⚠️ Limit amount ထည့်ပါ (ဥပမာ: /break 5000)")
            
    except Exception as e:
        logger.error(f"Error in break: {str(e)}")
        await update.message.reply_text(f"❌ Error: {str(e)}")

async def overbuy(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global admin_id, current_working_date
    try:
        if update.effective_user.id != admin_id:
            await update.message.reply_text("❌ Admin only command")
            return
            
        # Determine which date to work on
        date_key = current_working_date if current_working_date else get_current_date_key()
            
        if not context.args:
            await update.message.reply_text("ℹ️ /overbuy ကာဒိုင်အမည်ထည့်ပါ")
            return
            
        limit = await get_break_limit(date_key)
        if limit is None:
            await update.message.reply_text(f"⚠️ {date_key} အတွက် ကျေးဇူးပြု၍ /break [limit] ဖြင့် limit သတ်မှတ်ပါ")
            return
            
        # Get all bets for this date
        bets = await get_user_bets(date_key=date_key)
        if not bets:
            await update.message.reply_text(f"ℹ️ {date_key} အတွက် လောင်းကြေးမရှိသေးပါ")
            return
            
        username = context.args[0]
        context.user_data['overbuy_username'] = username
        context.user_data['overbuy_date'] = date_key
        
        # Calculate totals per number
        number_totals = {}
        for bet in bets:
            num = bet['number']
            amt = bet['amount']
            number_totals[num] = number_totals.get(num, 0) + amt
        
        # Find over-limit numbers
        over_numbers = {num: amt - limit for num, amt in number_totals.items() if amt > limit}
        
        if not over_numbers:
            await update.message.reply_text(f"ℹ️ {date_key} အတွက် ဘယ်ဂဏန်းမှ limit ({limit}) မကျော်ပါ")
            return
            
        if date_key not in overbuy_selections:
            overbuy_selections[date_key] = {}
        overbuy_selections[date_key][username] = over_numbers.copy()
        
        msg = [f"{username} ထံမှာတင်ရန်များ (Date: {date_key}, Limit: {limit}):"]
        buttons = []
        for num, amt in over_numbers.items():
            buttons.append([InlineKeyboardButton(f"{num:02d} ➤ {amt} {'✅' if num in overbuy_selections[date_key][username] else '⬜'}", 
                          callback_data=f"overbuy_select:{num}")])
        
        buttons.append([
            InlineKeyboardButton("Select All", callback_data="overbuy_select_all"),
            InlineKeyboardButton("Unselect All", callback_data="overbuy_unselect_all")
        ])
        buttons.append([InlineKeyboardButton("OK", callback_data="overbuy_confirm")])
        
        reply_markup = InlineKeyboardMarkup(buttons)
        await update.message.reply_text("\n".join(msg), reply_markup=reply_markup)
        
    except Exception as e:
        logger.error(f"Error in overbuy: {str(e)}")
        await update.message.reply_text(f"❌ Error: {str(e)}")

async def overbuy_select(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    try:
        _, num_str = query.data.split(':')
        num = int(num_str)
        username = context.user_data.get('overbuy_username')
        date_key = context.user_data.get('overbuy_date')
        
        if not username or not date_key:
            await query.edit_message_text("❌ Error: User or date not found")
            return
            
        if date_key not in overbuy_selections or username not in overbuy_selections[date_key]:
            await query.edit_message_text("❌ Error: Selection data not found")
            return
            
        if num in overbuy_selections[date_key][username]:
            del overbuy_selections[date_key][username][num]
        else:
            limit = await get_break_limit(date_key)
            if limit is None:
                await query.edit_message_text("❌ Error: No break limit set for this date")
                return
                
            # Get total for this number
            bets = await get_user_bets(date_key=date_key)
            if not bets:
                await query.edit_message_text("❌ Error: No bets found for this date")
                return
                
            total = sum(bet['amount'] for bet in bets if bet['number'] == num)
            overbuy_selections[date_key][username][num] = total - limit
            
        msg = [f"{username} ထံမှာတင်ရန်များ (Date: {date_key}):"]
        buttons = []
        for n, amt in overbuy_selections[date_key][username].items():
            buttons.append([InlineKeyboardButton(f"{n:02d} ➤ {amt} {'✅' if n in overbuy_selections[date_key][username] else '⬜'}", 
                          callback_data=f"overbuy_select:{n}")])
        
        buttons.append([
            InlineKeyboardButton("Select All", callback_data="overbuy_select_all"),
            InlineKeyboardButton("Unselect All", callback_data="overbuy_unselect_all")
        ])
        buttons.append([InlineKeyboardButton("OK", callback_data="overbuy_confirm")])
        
        reply_markup = InlineKeyboardMarkup(buttons)
        await query.edit_message_text("\n".join(msg), reply_markup=reply_markup)
        
    except Exception as e:
        logger.error(f"Error in overbuy_select: {str(e)}")
        await query.edit_message_text("❌ Error occurred")

async def overbuy_select_all(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    try:
        username = context.user_data.get('overbuy_username')
        date_key = context.user_data.get('overbuy_date')
        
        if not username or not date_key:
            await query.edit_message_text("❌ Error: User or date not found")
            return
            
        limit = await get_break_limit(date_key)
        if limit is None:
            await query.edit_message_text("❌ Error: No break limit set for this date")
            return
            
        # Get all bets for this date
        bets = await get_user_bets(date_key=date_key)
        if not bets:
            await query.edit_message_text("❌ Error: No bets found for this date")
            return
            
        # Calculate totals per number
        number_totals = {}
        for bet in bets:
            num = bet['number']
            amt = bet['amount']
            number_totals[num] = number_totals.get(num, 0) + amt
        
        # Initialize selections
        if date_key not in overbuy_selections:
            overbuy_selections[date_key] = {}
            
        overbuy_selections[date_key][username] = {
            num: amt - limit 
            for num, amt in number_totals.items() 
            if amt > limit
        }
        
        msg = [f"{username} ထံမှာတင်ရန်များ (Date: {date_key}):"]
        buttons = []
        for num, amt in overbuy_selections[date_key][username].items():
            buttons.append([InlineKeyboardButton(f"{num:02d} ➤ {amt} ✅", 
                          callback_data=f"overbuy_select:{num}")])
        
        buttons.append([
            InlineKeyboardButton("Select All", callback_data="overbuy_select_all"),
            InlineKeyboardButton("Unselect All", callback_data="overbuy_unselect_all")
        ])
        buttons.append([InlineKeyboardButton("OK", callback_data="overbuy_confirm")])
        
        reply_markup = InlineKeyboardMarkup(buttons)
        await query.edit_message_text("\n".join(msg), reply_markup=reply_markup)
        
    except Exception as e:
        logger.error(f"Error in overbuy_select_all: {str(e)}")
        await query.edit_message_text("❌ Error occurred")

async def overbuy_unselect_all(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    try:
        username = context.user_data.get('overbuy_username')
        date_key = context.user_data.get('overbuy_date')
        
        if not username or not date_key:
            await query.edit_message_text("❌ Error: User or date not found")
            return
            
        if date_key not in overbuy_selections:
            overbuy_selections[date_key] = {}
            
        overbuy_selections[date_key][username] = {}
        
        limit = await get_break_limit(date_key)
        if limit is None:
            await query.edit_message_text("❌ Error: No break limit set for this date")
            return
            
        # Get all bets for this date
        bets = await get_user_bets(date_key=date_key)
        if not bets:
            await query.edit_message_text("❌ Error: No bets found for this date")
            return
            
        # Calculate totals per number
        number_totals = {}
        for bet in bets:
            num = bet['number']
            amt = bet['amount']
            number_totals[num] = number_totals.get(num, 0) + amt
        
        # Find over-limit numbers
        over_numbers = {num: amt - limit for num, amt in number_totals.items() if amt > limit}
        
        msg = [f"{username} ထံမှာတင်ရန်များ (Date: {date_key}):"]
        buttons = []
        for num, amt in over_numbers.items():
            buttons.append([InlineKeyboardButton(f"{num:02d} ➤ {amt} ⬜", 
                          callback_data=f"overbuy_select:{num}")])
        
        buttons.append([
            InlineKeyboardButton("Select All", callback_data="overbuy_select_all"),
            InlineKeyboardButton("Unselect All", callback_data="overbuy_unselect_all")
        ])
        buttons.append([InlineKeyboardButton("OK", callback_data="overbuy_confirm")])
        
        reply_markup = InlineKeyboardMarkup(buttons)
        await query.edit_message_text("\n".join(msg), reply_markup=reply_markup)
        
    except Exception as e:
        logger.error(f"Error in overbuy_unselect_all: {str(e)}")
        await query.edit_message_text("❌ Error occurred")

async def overbuy_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    try:
        username = context.user_data.get('overbuy_username')
        date_key = context.user_data.get('overbuy_date')
        
        if not username or not date_key:
            await query.edit_message_text("❌ Error: User or date not found")
            return
            
        if date_key not in overbuy_selections or username not in overbuy_selections[date_key]:
            await query.edit_message_text("❌ Error: Selection data not found")
            return
            
        selected_numbers = overbuy_selections[date_key][username]
        if not selected_numbers:
            await query.edit_message_text("⚠️ ဘာဂဏန်းမှမရွေးထားပါ")
            return
            
        total_amount = 0
        bets = []
        for num, amt in selected_numbers.items():
            # Save negative amount to represent overbuy
            await save_user_bet(username, date_key, num, -amt)
            bets.append(f"{num:02d}-{amt}")
            total_amount += amt
        
        # Initialize overbuy_list for date if needed
        if date_key not in overbuy_list:
            overbuy_list[date_key] = {}
        overbuy_list[date_key][username] = selected_numbers.copy()
        
        response = f"{username} - {date_key}\n" + "\n".join(bets) + f"\nစုစုပေါင်း {total_amount} ကျပ်"
        await query.edit_message_text(response)
        
    except Exception as e:
        logger.error(f"Error in overbuy_confirm: {str(e)}")
        await query.edit_message_text("❌ Error occurred")

async def pnumber(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global admin_id, current_working_date
    try:
        if update.effective_user.id != admin_id:
            await update.message.reply_text("❌ Admin only command")
            return
            
        # Determine which date to work on
        date_key = current_working_date if current_working_date else get_current_date_key()
            
        if not context.args:
            pnum = await get_power_number(date_key)
            if pnum is not None:
                await update.message.reply_text(f"ℹ️ Usage: /pnumber [number]\nℹ️ {date_key} အတွက် Power Number: {pnum:02d}")
            else:
                await update.message.reply_text(f"ℹ️ Usage: /pnumber [number]\nℹ️ {date_key} အတွက် Power Number မသတ်မှတ်ရသေးပါ")
            return
            
        try:
            num = int(context.args[0])
            if num < 0 or num > 99:
                await update.message.reply_text("⚠️ ဂဏန်းကို 0 နှင့် 99 ကြားထည့်ပါ")
                return
                
            await save_power_number(date_key, num)
            await update.message.reply_text(f"✅ {date_key} အတွက် Power Number ကို {num:02d} အဖြစ်သတ်မှတ်ပြီး")
            
            # Show report for this date
            msg = []
            total_power = 0
            
            # Get all bets for this date
            bets = await get_user_bets(date_key=date_key)
            if bets:
                user_totals = {}
                for bet in bets:
                    if bet['number'] == num:
                        user = bet['username']
                        amt = bet['amount']
                        user_totals[user] = user_totals.get(user, 0) + amt
                        total_power += amt
                
                for user, amt in user_totals.items():
                    msg.append(f"{user}: {num:02d} ➤ {amt}")
            
            if msg:
                msg.append(f"\n🔴 {date_key} အတွက် Power Number စုစုပေါင်း: {total_power}")
                await update.message.reply_text("\n".join(msg))
            else:
                await update.message.reply_text(f"ℹ️ {date_key} အတွက် {num:02d} အတွက် လောင်းကြေးမရှိပါ")
                
        except ValueError:
            await update.message.reply_text("⚠️ ဂဏန်းမှန်မှန်ထည့်ပါ (ဥပမာ: /pnumber 15)")
            
    except Exception as e:
        logger.error(f"Error in pnumber: {str(e)}")
        await update.message.reply_text(f"❌ Error: {str(e)}")

async def comandza(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global admin_id
    try:
        if update.effective_user.id != admin_id:
            await update.message.reply_text("❌ Admin only command")
            return
            
        users = await get_all_users()
        if not users:
            await update.message.reply_text("ℹ️ လက်ရှိ user မရှိပါ")
            return
            
        keyboard = [[InlineKeyboardButton(u, callback_data=f"comza:{u}")] for u in users]
        await update.message.reply_text("👉 User ကိုရွေးပါ", reply_markup=InlineKeyboardMarkup(keyboard))
    except Exception as e:
        logger.error(f"Error in comandza: {str(e)}")
        await update.message.reply_text(f"❌ Error: {str(e)}")

async def comza_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        query = update.callback_query
        await query.answer()
        context.user_data['selected_user'] = query.data.split(":")[1]
        await query.edit_message_text(f"👉 {context.user_data['selected_user']} ကိုရွေးထားသည်။ 15/80 လို့ထည့်ပါ")
    except Exception as e:
        logger.error(f"Error in comza_input: {str(e)}")
        await query.edit_message_text(f"❌ Error: {str(e)}")

async def comza_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        user = context.user_data.get('selected_user')
        if not user:
            await handle_message(update, context)
            return
            
        text = update.message.text
        if text and '/' in text:
            try:
                parts = text.split('/')
                if len(parts) != 2:
                    raise ValueError
                
                com = int(parts[0])
                za = int(parts[1])
                
                if com < 0 or com > 100 or za < 0:
                    raise ValueError
                    
                await save_user_com_za(user, com, za)
                del context.user_data['selected_user']
                await update.message.reply_text(f"✅ Com {com}%, Za {za} မှတ်ထားပြီး")
            except:
                await update.message.reply_text("⚠️ မှန်မှန်ရေးပါ (ဥပမာ: 15/80)")
        else:
            await update.message.reply_text("⚠️ ဖော်မတ်မှားနေပါသည်။ 15/80 လို့ထည့်ပါ")
    except Exception as e:
        logger.error(f"Error in comza_text: {str(e)}")
        await update.message.reply_text(f"❌ Error: {str(e)}")

async def total(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global admin_id, current_working_date
    try:
        if update.effective_user.id != admin_id:
            await update.message.reply_text("❌ Admin only command")
            return
            
        # Determine which date to work on
        date_key = current_working_date if current_working_date else get_current_date_key()
            
        pnum = await get_power_number(date_key)
        if pnum is None:
            await update.message.reply_text(f"⚠️ {date_key} အတွက် ကျေးဇူးပြု၍ /pnumber [number] ဖြင့် Power Number သတ်မှတ်ပါ")
            return
            
        # Get all bets for this date
        bets = await get_user_bets(date_key=date_key)
        if not bets:
            await update.message.reply_text(f"ℹ️ {date_key} အတွက် လောင်းကြေးမရှိပါ")
            return
            
        msg = [f"📊 {date_key} အတွက် စုပေါင်းရလဒ်"]
        total_net = 0
        
        # Group bets by user
        user_totals = {}
        user_power = {}
        
        for bet in bets:
            user = bet['username']
            num = bet['number']
            amt = bet['amount']
            
            # Total amount
            user_totals[user] = user_totals.get(user, 0) + amt
            
            # Power number amount
            if num == pnum:
                user_power[user] = user_power.get(user, 0) + amt
        
        # Calculate for each user
        for user, total_amt in user_totals.items():
            com, za = await get_user_com_za(user)
            commission_amt = (total_amt * com) // 100
            after_com = total_amt - commission_amt
            win_amt = user_power.get(user, 0) * za
            
            net = after_com - win_amt
            status = "ဒိုင်ကပေးရမည်" if net < 0 else "ဒိုင်ကရမည်"
            
            user_report = (
                f"👤 {user}\n"
                f"💵 စုစုပေါင်း: {total_amt}\n"
                f"📊 Com({com}%) ➤ {commission_amt}\n"
                f"💰 Com ပြီး: {after_com}\n"
                f"🔢 Power Number({pnum:02d}) ➤ {user_power.get(user, 0)}\n"
                f"🎯 Za({za}) ➤ {win_amt}\n"
                f"📈 ရလဒ်: {abs(net)} ({status})\n"
                "-----------------"
            )
            msg.append(user_report)
            total_net += net

        if len(msg) > 1:
            msg.append(f"\n📊 စုစုပေါင်းရလဒ်: {abs(total_net)} ({'ဒိုင်အရှုံး' if total_net < 0 else 'ဒိုင်အမြတ်'})")
            await update.message.reply_text("\n".join(msg))
        else:
            await update.message.reply_text(f"ℹ️ {date_key} အတွက် ဒေတာမရှိပါ")
    except Exception as e:
        logger.error(f"Error in total: {str(e)}")
        await update.message.reply_text(f"❌ Error: {str(e)}")

async def tsent(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global admin_id, current_working_date
    try:
        if update.effective_user.id != admin_id:
            await update.message.reply_text("❌ Admin only command")
            return
            
        # Determine which date to work on
        date_key = current_working_date if current_working_date else get_current_date_key()
            
        # Get all bets for this date
        bets = await get_user_bets(date_key=date_key)
        if not bets:
            await update.message.reply_text(f"ℹ️ {date_key} အတွက် လောင်းကြေးမရှိပါ")
            return
            
        # Group by user
        user_bets = {}
        for bet in bets:
            user = bet['username']
            if user not in user_bets:
                user_bets[user] = []
            user_bets[user].append((bet['number'], bet['amount']))
        
        for user, bets in user_bets.items():
            user_report = [f"👤 {user} - {date_key}:"]
            total_amt = 0
                
            for num, amt in bets:
                user_report.append(f"  - {num:02d} ➤ {amt}")
                total_amt += amt
            
            user_report.append(f"💵 စုစုပေါင်း: {total_amt}")
            await update.message.reply_text("\n".join(user_report))
        
        await update.message.reply_text(f"✅ {date_key} အတွက် စာရင်းများအားလုံး ပေးပို့ပြီးပါပြီ")
    except Exception as e:
        logger.error(f"Error in tsent: {str(e)}")
        await update.message.reply_text(f"❌ Error: {str(e)}")
        
async def alldata(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global admin_id
    try:
        if update.effective_user.id != admin_id:
            await update.message.reply_text("❌ Admin only command")
            return
            
        users = await get_all_users()
        if not users:
            await update.message.reply_text("ℹ️ လက်ရှိစာရင်းမရှိပါ")
            return
            
        msg = ["📊 **စာရင်းသွင်းထားသော User များ**"]
        msg.append("⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯")
        
        for user in users:
            com, za = await get_user_com_za(user)
            msg.append(f"👤 **{user}**\n   - Com: {com}%\n   - Za: {za}x")
        
        keyboard = [[InlineKeyboardButton("➕ Add User", callback_data="add_user")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await update.message.reply_text(
            "\n".join(msg),
            reply_markup=reply_markup
        )
    except Exception as e:
        logger.error(f"Error in alldata: {str(e)}")
        await update.message.reply_text(f"❌ Error: {str(e)}")

async def add_user_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    await query.edit_message_text("ℹ️ User အသစ်ထည့်ရန်:\nဖော်မတ်: `<အမည်>@<Com>@<Za>`\nဥပမာ: `မမ@15@80`")

async def handle_new_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        text = update.message.text
        if "@" not in text:
            await update.message.reply_text("❌ ဖော်မတ်မှားနေပါသည်။ ဥပမာ: `မမ@15@80`")
            return
        
        username, com_str, za_str = text.split("@")
        com = int(com_str)
        za = int(za_str)
        
        await save_user_com_za(username, com, za)
        
        await update.message.reply_text(
            f"✅ User အသစ်ထည့်ပြီးပါပြီ!\n"
            f"👤 {username}\n"
            f"   - Com: {com}%\n"
            f"   - Za: {za}x"
        )
        
        await alldata(update, context)
        
    except Exception as e:
        logger.error(f"Error adding user: {str(e)}")
        await update.message.reply_text("❌ Error! ဖော်မတ်မှားနေပါသည်။ ဥပမာ: `မမ@15@80`")

async def reset_data(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global admin_id, date_control, overbuy_list, overbuy_selections, current_working_date, closed_numbers
    
    try:
        if update.effective_user.id != admin_id:
            await update.message.reply_text("❌ Admin only command")
            return
            
        # Clear non-persistent data
        date_control = {}
        overbuy_list = {}
        overbuy_selections = {}
        closed_numbers = set()
        current_working_date = get_current_date_key()
        
        await update.message.reply_text("✅ မှတ်ဉာဏ်အတွင်းရှိ ဒေတာများကို ပြန်လည်သုတ်သင်ပြီး လက်ရှိနေ့သို့ပြန်လည်သတ်မှတ်ပြီးပါပြီ\n\nℹ️ Database ထဲက data တွေကိုတော့ မဖျက်ပါ")
    except Exception as e:
        logger.error(f"Error in reset_data: {str(e)}")
        await update.message.reply_text(f"❌ Error: {str(e)}")
        
async def posthis(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        user = update.effective_user
        is_admin = user.id == admin_id
        
        if is_admin and not context.args:
            users = await get_all_users()
            if not users:
                await update.message.reply_text("ℹ️ လက်ရှိ user မရှိပါ")
                return
                
            keyboard = [[InlineKeyboardButton(u, callback_data=f"posthis:{u}")] for u in users]
            await update.message.reply_text(
                "ဘယ် user ရဲ့စာရင်းကိုကြည့်မလဲ?",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            return
        
        username = user.username if not is_admin else context.args[0] if context.args else None
        
        if not username:
            await update.message.reply_text("❌ User မရှိပါ")
            return
            
        # For non-admin, show current date only
        date_key = get_current_date_key() if not is_admin else None
        
        msg = [f"📊 {username} ရဲ့လောင်းကြေးမှတ်တမ်း"]
        total_amount = 0
        pnumber_total = 0
        
        if is_admin:
            # Admin can see all dates - get all bets for this user
            bets = await get_user_bets(username=username)
            if not bets:
                await update.message.reply_text(f"ℹ️ {username} အတွက် စာရင်းမရှိပါ")
                return
                
            # Group by date
            date_bets = {}
            for bet in bets:
                date_key = bet['date_key']
                if date_key not in date_bets:
                    date_bets[date_key] = []
                date_bets[date_key].append((bet['number'], bet['amount']))
            
            for date_key, bets in date_bets.items():
                pnum = await get_power_number(date_key)
                pnum_str = f" [P: {pnum:02d}]" if pnum is not None else ""
                
                msg.append(f"\n📅 {date_key}{pnum_str}:")
                for num, amt in bets:
                    if pnum is not None and num == pnum:
                        msg.append(f"🔴 {num:02d} ➤ {amt} 🔴")
                        pnumber_total += amt
                    else:
                        msg.append(f"{num:02d} ➤ {amt}")
                    total_amount += amt
        else:
            # Non-admin only sees current date
            bets = await get_user_bets(username=username, date_key=date_key)
            if bets:
                pnum = await get_power_number(date_key)
                pnum_str = f" [P: {pnum:02d}]" if pnum is not None else ""
                
                msg.append(f"\n📅 {date_key}{pnum_str}:")
                for bet in bets:
                    num = bet['number']
                    amt = bet['amount']
                    if pnum is not None and num == pnum:
                        msg.append(f"🔴 {num:02d} ➤ {amt} 🔴")
                        pnumber_total += amt
                    else:
                        msg.append(f"{num:02d} ➤ {amt}")
                    total_amount += amt
        
        if len(msg) > 1:
            msg.append(f"\n💵 စုစုပေါင်း: {total_amount}")
            if pnumber_total > 0:
                msg.append(f"🔴 Power Number စုစုပေါင်း: {pnumber_total}")
            await update.message.reply_text("\n".join(msg))
        else:
            await update.message.reply_text(f"ℹ️ {username} အတွက် စာရင်းမရှိပါ")
        
    except Exception as e:
        logger.error(f"Error in posthis: {str(e)}")
        await update.message.reply_text(f"❌ Error: {str(e)}")

async def posthis_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    try:
        _, username = query.data.split(':')
        msg = [f"📊 {username} ရဲ့လောင်းကြေးမှတ်တမ်း"]
        total_amount = 0
        pnumber_total = 0
        
        # Get all bets for this user
        bets = await get_user_bets(username=username)
        if bets:
            # Group by date
            date_bets = {}
            for bet in bets:
                date_key = bet['date_key']
                if date_key not in date_bets:
                    date_bets[date_key] = []
                date_bets[date_key].append((bet['number'], bet['amount']))
            
            for date_key, bets in date_bets.items():
                pnum = await get_power_number(date_key)
                pnum_str = f" [P: {pnum:02d}]" if pnum is not None else ""
                
                msg.append(f"\n📅 {date_key}{pnum_str}:")
                for num, amt in bets:
                    if pnum is not None and num == pnum:
                        msg.append(f"🔴 {num:02d} ➤ {amt} 🔴")
                        pnumber_total += amt
                    else:
                        msg.append(f"{num:02d} ➤ {amt}")
                    total_amount += amt
            
            if len(msg) > 1:
                msg.append(f"\n💵 စုစုပေါင်း: {total_amount}")
                if pnumber_total > 0:
                    msg.append(f"🔴 Power Number စုစုပေါင်း: {pnumber_total}")
                await query.edit_message_text("\n".join(msg))
            else:
                await query.edit_message_text(f"ℹ️ {username} အတွက် စာရင်းမရှိပါ")
        else:
            await query.edit_message_text(f"ℹ️ {username} အတွက် စာရင်းမရှိပါ")
            
    except Exception as e:
        logger.error(f"Error in posthis_callback: {str(e)}")
        await query.edit_message_text("❌ Error occurred")

async def dateall(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global admin_id
    try:
        if update.effective_user.id != admin_id:
            await update.message.reply_text("❌ Admin only command")
            return
            
        # Get all unique dates from database
        all_dates = await get_available_dates()
        
        if not all_dates:
            await update.message.reply_text("ℹ️ မည်သည့်စာရင်းမှ မရှိသေးပါ")
            return
            
        # Initialize selection dictionary
        dateall_selections = {date: False for date in all_dates}
        context.user_data['dateall_selections'] = dateall_selections
        
        # Build message with checkboxes
        msg = ["📅 စာရင်းရှိသည့်နေ့ရက်များကို ရွေးချယ်ပါ:"]
        buttons = []
        
        for date in all_dates:
            pnum = await get_power_number(date)
            pnum_str = f" [P: {pnum:02d}]" if pnum is not None else ""
            
            is_selected = dateall_selections[date]
            button_text = f"{date}{pnum_str} {'✅' if is_selected else '⬜'}"
            buttons.append([InlineKeyboardButton(button_text, callback_data=f"dateall_toggle:{date}")])
        
        buttons.append([InlineKeyboardButton("👁‍🗨 View", callback_data="dateall_view")])
        reply_markup = InlineKeyboardMarkup(buttons)
        
        await update.message.reply_text("\n".join(msg), reply_markup=reply_markup)
        
    except Exception as e:
        logger.error(f"Error in dateall: {str(e)}")
        await update.message.reply_text(f"❌ Error: {str(e)}")

async def dateall_toggle(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    try:
        _, date_key = query.data.split(':')
        dateall_selections = context.user_data.get('dateall_selections', {})
        
        if date_key not in dateall_selections:
            await query.edit_message_text("❌ Error: Date not found")
            return
            
        # Toggle selection status
        dateall_selections[date_key] = not dateall_selections[date_key]
        context.user_data['dateall_selections'] = dateall_selections
        
        # Rebuild the message with updated selections
        msg = ["📅 စာရင်းရှိသည့်နေ့ရက်များကို ရွေးချယ်ပါ:"]
        buttons = []
        
        for date in dateall_selections.keys():
            pnum = await get_power_number(date)
            pnum_str = f" [P: {pnum:02d}]" if pnum is not None else ""
            
            is_selected = dateall_selections[date]
            button_text = f"{date}{pnum_str} {'✅' if is_selected else '⬜'}"
            buttons.append([InlineKeyboardButton(button_text, callback_data=f"dateall_toggle:{date}")])
        
        buttons.append([InlineKeyboardButton("👁‍🗨 View", callback_data="dateall_view")])
        reply_markup = InlineKeyboardMarkup(buttons)
        
        await query.edit_message_text("\n".join(msg), reply_markup=reply_markup)
        
    except Exception as e:
        logger.error(f"Error in dateall_toggle: {str(e)}")
        await query.edit_message_text("❌ Error occurred")

async def dateall_view(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    try:
        # 1. Get selected dates
        dateall_selections = context.user_data.get('dateall_selections', {})
        selected_dates = [date for date, selected in dateall_selections.items() if selected]
        
        if not selected_dates:
            await query.edit_message_text("⚠️ မည်သည့်နေ့ရက်ကိုမှ မရွေးချယ်ထားပါ")
            return

        # 2. Initialize data storage
        user_reports = {}  # {username: {'total_bet': 0, 'power_bet': 0, 'com': X, 'za': Y}}
        grand_totals = {
            'total_bet': 0,
            'power_bet': 0,
            'commission': 0,
            'win_amount': 0,
            'net_result': 0
        }

        # 3. Process bets WITHOUT overbuy adjustment
        for date_key in selected_dates:
            bets = await get_user_bets(date_key=date_key)
            if not bets:
                continue
                
            pnum = await get_power_number(date_key)
            
            for bet in bets:
                username = bet['username']
                num = bet['number']
                amt = bet['amount']
                
                if username not in user_reports:
                    com, za = await get_user_com_za(username)
                    user_reports[username] = {
                        'total_bet': 0,
                        'power_bet': 0,
                        'com': com,
                        'za': za
                    }
                
                # Track total bets (ignore negative amounts which are overbuys)
                if amt > 0:
                    user_reports[username]['total_bet'] += amt
                    
                # Track power number bets
                if pnum is not None and num == pnum and amt > 0:
                    user_reports[username]['power_bet'] += amt

        # 4. Calculate financials
        messages = ["📊 ရွေးချယ်ထားသော နေ့ရက်များ စုစုပေါင်းရလဒ် (Overbuy မပါ)"]
        messages.append(f"📅 ရက်စွဲများ: {', '.join(selected_dates)}\n")
        
        for username, report in user_reports.items():
            # Calculate values
            commission = (report['total_bet'] * report['com']) // 100
            after_com = report['total_bet'] - commission
            win_amount = report['power_bet'] * report['za']
            net_result = after_com - win_amount
            
            # Build user message
            user_msg = [
                f"👤 {username}",
                f"💵 စုစုပေါင်းလောင်းကြေး: {report['total_bet']}",
                f"📊 Com ({report['com']}%): {commission}",
                f"💰 Com ပြီး: {after_com}"
            ]
            
            if report['power_bet'] > 0:
                user_msg.extend([
                    f"🔴 Power Number: {report['power_bet']}",
                    f"🎯 Za ({report['za']}): {win_amount}"
                ])
            
            user_msg.append(
                f"📈 ရလဒ်: {abs(net_result)} ({'ဒိုင်ကပေးရန်' if net_result < 0 else 'ဒိုင်ကရမည်'})"
            )
            
            messages.append("\n".join(user_msg))
            messages.append("⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯")
            
            # Update grand totals
            grand_totals['total_bet'] += report['total_bet']
            grand_totals['power_bet'] += report['power_bet']
            grand_totals['commission'] += commission
            grand_totals['win_amount'] += win_amount
            grand_totals['net_result'] += net_result

        # 5. Add grand totals
        messages.append("\n📌 စုစုပေါင်းရလဒ်:")
        messages.append(f"💵 စုစုပေါင်းလောင်းကြေး: {grand_totals['total_bet']}")
        messages.append(f"📊 Com စုစုပေါင်း: {grand_totals['commission']}")
        
        if grand_totals['power_bet'] > 0:
            messages.append(f"🔴 Power Number စုစုပေါင်း: {grand_totals['power_bet']}")
            messages.append(f"🎯 Win Amount စုစုပေါင်း: {grand_totals['win_amount']}")
        
        messages.append(
            f"📊 စုစုပေါင်းရလဒ်: {abs(grand_totals['net_result'])} "
            f"({'ဒိုင်အရှုံး' if grand_totals['net_result'] < 0 else 'ဒိုင်အမြတ်'})"
        )

        # 6. Send message (split if too long)
        full_message = "\n".join(messages)
        if len(full_message) > 4000:
            half = len(messages) // 2
            await query.edit_message_text("\n".join(messages[:half]))
            await context.bot.send_message(
                chat_id=query.message.chat_id,
                text="\n".join(messages[half:])
            )
        else:
            await query.edit_message_text(full_message)

    except Exception as e:
        logger.error(f"Error in dateall_view: {str(e)}")
        await query.edit_message_text("❌ တွက်ချက်မှုအမှားဖြစ်နေပါသည်")
        
async def change_working_date(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global admin_id
    try:
        if update.effective_user.id != admin_id:
            await update.message.reply_text("❌ Admin only command")
            return
        
        # Show calendar with AM/PM selection
        keyboard = [
            [InlineKeyboardButton("🗓 လက်ရှိလအတွက် ပြက္ခဒိန်", callback_data="cdate_calendar")],
            [InlineKeyboardButton("⏰ AM ရွေးရန်", callback_data="cdate_am")],
            [InlineKeyboardButton("🌙 PM ရွေးရန်", callback_data="cdate_pm")],
            [InlineKeyboardButton("📆 ယနေ့ဖွင့်ရန်", callback_data="cdate_open")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.message.reply_text(
            "👉 လက်ရှိ အလုပ်လုပ်ရမည့်နေ့ရက်ကို ရွေးချယ်ပါ\n"
            "• ပြက္ခဒိန်ဖြင့်ရွေးရန်: 🗓 ခလုတ်ကိုနှိပ်ပါ\n"
            "• AM သို့ပြောင်းရန်: ⏰ ခလုတ်ကိုနှိပ်ပါ\n"
            "• PM သို့ပြောင်းရန်: 🌙 ခလုတ်ကိုနှိပ်ပါ\n"
            "• ယနေ့သို့ပြန်ရန်: 📆 ခလုတ်ကိုနှိပ်ပါ",
            reply_markup=reply_markup
        )
    except Exception as e:
        logger.error(f"Error in change_working_date: {str(e)}")
        await update.message.reply_text(f"❌ Error: {str(e)}")

async def show_calendar(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    try:
        now = datetime.now(MYANMAR_TIMEZONE)
        year, month = now.year, now.month
        
        # Create calendar header
        cal_header = calendar.month_name[month] + " " + str(year)
        days = ["တနင်္လာ", "အင်္ဂါ", "ဗုဒ္ဓဟူး", "ကြာသပတေး", "သောကြာ", "စနေ", "တနင်္ဂနွေ"]
        
        # Generate calendar days
        cal = calendar.monthcalendar(year, month)
        keyboard = []
        keyboard.append([InlineKeyboardButton(cal_header, callback_data="ignore")])
        keyboard.append([InlineKeyboardButton(day, callback_data="ignore") for day in days])
        
        for week in cal:
            week_buttons = []
            for day in week:
                if day == 0:
                    week_buttons.append(InlineKeyboardButton(" ", callback_data="ignore"))
                else:
                    date_str = f"{day:02d}/{month:02d}/{year}"
                    week_buttons.append(InlineKeyboardButton(str(day), callback_data=f"cdate_day:{date_str}"))
            keyboard.append(week_buttons)
        
        # Add navigation and back buttons
        keyboard.append([
            InlineKeyboardButton("⬅️ ယခင်", callback_data="cdate_prev_month"),
            InlineKeyboardButton("➡️ နောက်", callback_data="cdate_next_month")
        ])
        keyboard.append([InlineKeyboardButton("🔙 နောက်သို့", callback_data="cdate_back")])
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text("🗓 နေ့ရက်ရွေးရန် ပြက္ခဒိန်", reply_markup=reply_markup)
        
    except Exception as e:
        logger.error(f"Error in show_calendar: {str(e)}")
        await query.edit_message_text("❌ Error occurred")

async def handle_day_selection(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    try:
        _, date_str = query.data.split(':')
        context.user_data['selected_date'] = date_str
        
        # Ask for AM/PM selection
        keyboard = [
            [InlineKeyboardButton("⏰ AM", callback_data="cdate_set_am")],
            [InlineKeyboardButton("🌙 PM", callback_data="cdate_set_pm")],
            [InlineKeyboardButton("🔙 နောက်သို့", callback_data="cdate_back")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text(
            f"👉 {date_str} အတွက် အချိန်ပိုင်းရွေးပါ",
            reply_markup=reply_markup
        )
        
    except Exception as e:
        logger.error(f"Error in handle_day_selection: {str(e)}")
        await query.edit_message_text("❌ Error occurred")

async def set_am_pm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    try:
        global current_working_date
        time_segment = "AM" if "am" in query.data else "PM"
        date_str = context.user_data.get('selected_date', '')
        
        if not date_str:
            await query.edit_message_text("❌ Error: Date not selected")
            return
            
        current_working_date = f"{date_str} {time_segment}"
        await query.edit_message_text(f"✅ လက်ရှိ အလုပ်လုပ်ရမည့်နေ့ရက်ကို {current_working_date} အဖြစ်ပြောင်းလိုက်ပါပြီ")
        
    except Exception as e:
        logger.error(f"Error in set_am_pm: {str(e)}")
        await query.edit_message_text("❌ Error occurred")

async def set_am(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global current_working_date
    try:
        if current_working_date:
            date_part = current_working_date.split()[0]
            current_working_date = f"{date_part} AM"
            await update.callback_query.edit_message_text(f"✅ လက်ရှိ အလုပ်လုပ်ရမည့်နေ့ရက်ကို {current_working_date} အဖြစ်ပြောင်းလိုက်ပါပြီ")
        else:
            await update.callback_query.edit_message_text("❌ လက်ရှိနေ့ရက် သတ်မှတ်ထားခြင်းမရှိပါ")
    except Exception as e:
        logger.error(f"Error in set_am: {str(e)}")
        await update.callback_query.edit_message_text("❌ Error occurred")

async def set_pm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global current_working_date
    try:
        if current_working_date:
            date_part = current_working_date.split()[0]
            current_working_date = f"{date_part} PM"
            await update.callback_query.edit_message_text(f"✅ လက်ရှိ အလုပ်လုပ်ရမည့်နေ့ရက်ကို {current_working_date} အဖြစ်ပြောင်းလိုက်ပါပြီ")
        else:
            await update.callback_query.edit_message_text("❌ လက်ရှိနေ့ရက် သတ်မှတ်ထားခြင်းမရှိပါ")
    except Exception as e:
        logger.error(f"Error in set_pm: {str(e)}")
        await update.callback_query.edit_message_text("❌ Error occurred")

async def open_current_date(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    try:
        global current_working_date
        current_working_date = get_current_date_key()
        await query.edit_message_text(f"✅ လက်ရှိ အလုပ်လုပ်ရမည့်နေ့ရက်ကို {current_working_date} အဖြစ်ပြောင်းလိုက်ပါပြီ")
    except Exception as e:
        logger.error(f"Error in open_current_date: {str(e)}")
        await query.edit_message_text("❌ Error occurred")

async def navigate_month(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Placeholder for month navigation
    await update.callback_query.answer()
    await update.callback_query.edit_message_text("ℹ️ လများလှန်ကြည့်ခြင်းအား နောက်ထပ်ဗားရှင်းတွင် ထည့်သွင်းပါမည်")

async def back_to_main(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await change_working_date(update, context)

async def delete_date(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global admin_id
    try:
        if update.effective_user.id != admin_id:
            await update.message.reply_text("❌ Admin only command")
            return
            
        # Get all available dates from database
        available_dates = await get_available_dates()
        
        if not available_dates:
            await update.message.reply_text("ℹ️ မည်သည့်စာရင်းမှ မရှိသေးပါ")
            return
            
        # Initialize selection dictionary
        datedelete_selections = {date: False for date in available_dates}
        context.user_data['datedelete_selections'] = datedelete_selections
        
        # Build message with checkboxes
        msg = ["🗑 ဖျက်လိုသောနေ့ရက်များကို ရွေးချယ်ပါ:"]
        buttons = []
        
        for date in available_dates:
            pnum = await get_power_number(date)
            pnum_str = f" [P: {pnum:02d}]" if pnum is not None else ""
            
            is_selected = datedelete_selections[date]
            button_text = f"{date}{pnum_str} {'✅' if is_selected else '⬜'}"
            buttons.append([InlineKeyboardButton(button_text, callback_data=f"datedelete_toggle:{date}")])
        
        buttons.append([InlineKeyboardButton("✅ Delete Selected", callback_data="datedelete_confirm")])
        reply_markup = InlineKeyboardMarkup(buttons)
        
        await update.message.reply_text("\n".join(msg), reply_markup=reply_markup)
        
    except Exception as e:
        logger.error(f"Error in delete_date: {str(e)}")
        await update.message.reply_text(f"❌ Error: {str(e)}")

async def datedelete_toggle(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    try:
        _, date_key = query.data.split(':')
        datedelete_selections = context.user_data.get('datedelete_selections', {})
        
        if date_key not in datedelete_selections:
            await query.edit_message_text("❌ Error: Date not found")
            return
            
        # Toggle selection status
        datedelete_selections[date_key] = not datedelete_selections[date_key]
        context.user_data['datedelete_selections'] = datedelete_selections
        
        # Rebuild the message with updated selections
        msg = ["🗑 ဖျက်လိုသောနေ့ရက်များကို ရွေးချယ်ပါ:"]
        buttons = []
        
        for date in datedelete_selections.keys():
            pnum = await get_power_number(date)
            pnum_str = f" [P: {pnum:02d}]" if pnum is not None else ""
            
            is_selected = datedelete_selections[date]
            button_text = f"{date}{pnum_str} {'✅' if is_selected else '⬜'}"
            buttons.append([InlineKeyboardButton(button_text, callback_data=f"datedelete_toggle:{date}")])
        
        buttons.append([InlineKeyboardButton("✅ Delete Selected", callback_data="datedelete_confirm")])
        reply_markup = InlineKeyboardMarkup(buttons)
        
        await query.edit_message_text("\n".join(msg), reply_markup=reply_markup)
        
    except Exception as e:
        logger.error(f"Error in datedelete_toggle: {str(e)}")
        await query.edit_message_text("❌ Error occurred")

async def datedelete_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    try:
        datedelete_selections = context.user_data.get('datedelete_selections', {})
        
        # Get selected dates
        selected_dates = [date for date, selected in datedelete_selections.items() if selected]
        
        if not selected_dates:
            await query.edit_message_text("⚠️ မည်သည့်နေ့ရက်ကိုမှ မရွေးချယ်ထားပါ")
            return
            
        # Delete data for selected dates
        for date_key in selected_dates:
            await delete_date_data(date_key)
        
        # Clear current working date if it was deleted
        global current_working_date
        if current_working_date in selected_dates:
            current_working_date = None
        
        await query.edit_message_text(f"✅ အောက်ပါနေ့ရက်များ ဖျက်ပြီးပါပြီ:\n{', '.join(selected_dates)}")
        
    except Exception as e:
        logger.error(f"Error in datedelete_confirm: {str(e)}")
        await query.edit_message_text("❌ Error occurred")

if __name__ == "__main__":
    if not TOKEN:
        raise ValueError("❌ BOT_TOKEN environment variable is not set")
        
    app = ApplicationBuilder().token(TOKEN).build()

    # ================= Command Handlers =================
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("menu", show_menu))
    app.add_handler(CommandHandler("dateopen", dateopen))
    app.add_handler(CommandHandler("dateclose", dateclose))
    app.add_handler(CommandHandler("ledger", ledger_summary))
    app.add_handler(CommandHandler("break", break_command))
    app.add_handler(CommandHandler("overbuy", overbuy))
    app.add_handler(CommandHandler("pnumber", pnumber))
    app.add_handler(CommandHandler("comandza", comandza))
    app.add_handler(CommandHandler("total", total))
    app.add_handler(CommandHandler("tsent", tsent))
    app.add_handler(CommandHandler("alldata", alldata))
    app.add_handler(CommandHandler("reset", reset_data))
    app.add_handler(CommandHandler("posthis", posthis))
    app.add_handler(CommandHandler("dateall", dateall))
    app.add_handler(CommandHandler("Cdate", change_working_date))
    app.add_handler(CommandHandler("Ddate", delete_date))
    app.add_handler(CommandHandler("numclose", numclose))

    # ================= Callback Handlers =================
    app.add_handler(CallbackQueryHandler(comza_input, pattern=r"^comza:"))
    app.add_handler(CallbackQueryHandler(delete_bet, pattern=r"^delete:"))
    app.add_handler(CallbackQueryHandler(confirm_delete, pattern=r"^confirm_delete:"))
    app.add_handler(CallbackQueryHandler(cancel_delete, pattern=r"^cancel_delete:"))
    app.add_handler(CallbackQueryHandler(overbuy_select, pattern=r"^overbuy_select:"))
    app.add_handler(CallbackQueryHandler(overbuy_select_all, pattern=r"^overbuy_select_all$"))
    app.add_handler(CallbackQueryHandler(overbuy_unselect_all, pattern=r"^overbuy_unselect_all$"))
    app.add_handler(CallbackQueryHandler(overbuy_confirm, pattern=r"^overbuy_confirm$"))
    app.add_handler(CallbackQueryHandler(posthis_callback, pattern=r"^posthis:"))
    app.add_handler(CallbackQueryHandler(dateall_toggle, pattern=r"^dateall_toggle:"))
    app.add_handler(CallbackQueryHandler(dateall_view, pattern=r"^dateall_view$"))
    app.add_handler(CallbackQueryHandler(numclose_delete_all, pattern=r"^numclose_delete_all$"))
    app.add_handler(CallbackQueryHandler(add_user_callback, pattern=r"^add_user$"))
    
    # Calendar handlers
    app.add_handler(CallbackQueryHandler(show_calendar, pattern=r"^cdate_calendar$"))
    app.add_handler(CallbackQueryHandler(handle_day_selection, pattern=r"^cdate_day:"))
    app.add_handler(CallbackQueryHandler(set_am, pattern=r"^cdate_am$"))
    app.add_handler(CallbackQueryHandler(set_pm, pattern=r"^cdate_pm$"))
    app.add_handler(CallbackQueryHandler(set_am_pm, pattern=r"^cdate_set_am$|^cdate_set_pm$"))
    app.add_handler(CallbackQueryHandler(open_current_date, pattern=r"^cdate_open$"))
    app.add_handler(CallbackQueryHandler(navigate_month, pattern=r"^cdate_prev_month$|^cdate_next_month$"))
    app.add_handler(CallbackQueryHandler(back_to_main, pattern=r"^cdate_back$"))
    
    app.add_handler(CallbackQueryHandler(datedelete_toggle, pattern=r"^datedelete_toggle:"))
    app.add_handler(CallbackQueryHandler(datedelete_confirm, pattern=r"^datedelete_confirm$"))

    # ================= Message Handlers =================
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND & filters.Regex(r'^[\u1000-\u109F\s]+$'), handle_menu_selection))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND & filters.Regex(r'^[^@]+@\d+@\d+$'), handle_new_user))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, comza_text))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    logger.info("🚀 Bot is starting...")
    app.run_polling()
