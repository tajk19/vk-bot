import json
import logging
from collections import defaultdict
from datetime import datetime, timedelta, timezone 
from typing import Any, Dict, List, Optional
from vkbottle.bot import Message, BotLabeler, Bot

from config import (
    ADMIN_IDS,
    SLOT_INTERVAL_MIN,
    format_date_with_weekday,
)
from google_sheets import (
    get_bookings,
    time_of_begining,
    time_of_end,
    ACTIVE_STATUSES,
)
from keyboards import(
    paginate_buttons,
)

class Role():
    
    logger = logging.getLogger(__name__)
    
    commands = {}
    
    context: Dict[int, Dict[str, Any]] = defaultdict(dict)
    
    
    def extract_payload(self, message: Message) -> Dict[str, Any]:
        payload = message.payload
        if not payload:
            return {}
        if isinstance(payload, dict):
            return payload
        if isinstance(payload, str):
            try:
                return json.loads(payload)
            except (json.JSONDecodeError, TypeError):
                return {}
        return {}
    
    def is_admin(self, message: Message) -> bool:
        return message.from_id in ADMIN_IDS
    
    def is_user(self, message: Message) -> bool:
        return message.from_id not in ADMIN_IDS

    def normalize(self, text: Optional[str]) -> str:
        return (text or "").strip().lower()
    
    def reset_context(self, id: int) -> None:
        self.context.pop(id, None)
    
    
    def all_time_slots(self) -> List[str]:
        return [
            f"{minutes // 60:02d}:{minutes % 60:02d}"
            for minutes in range(0, 24 * 60, SLOT_INTERVAL_MIN)
        ]

    def free_times_for_date(
        self,
        selected_date: datetime.date,
        active_bookings: Optional[List[Dict[str, str]]] = None,
    ) -> List[str]:
        if active_bookings is None:
            bookings = get_bookings(date=selected_date, statuses=ACTIVE_STATUSES)
        else:
            date_str = selected_date
            bookings = [
                booking for booking in active_bookings if booking.get("Дата") == date_str
            ]
        existing = {booking["Время"] for booking in bookings}

        offset = timedelta(hours=3)
        dt = timezone(offset, name='МСК')
        now = datetime.now(dt)
        
        slots: List[str] = []
        
        # Определяем время начала в зависимости от дня недели
        # 0 = понедельник, 6 = воскресенье
        current_weekday = selected_date.weekday()

        # Начальное время
        start_hour = time_of_begining(current_weekday)

        # Конечное время
        end_hour = time_of_end(current_weekday) 
        
        
        for time_slot in self.all_time_slots():
            # Пропускаем слоты до начала рабочего времени
            slot_hour = int(time_slot[:2])
            if slot_hour < start_hour:
                continue
            
            # Пропускаем слоты после окончания рабочего времени
            if slot_hour >= end_hour:
                continue
                
            if time_slot in existing:
                continue
                
            # Проверяем, не прошел ли уже временной слот для текущей даты
            if selected_date == now.date():
                slot_minutes = int(time_slot[:2]) * 60 + int(time_slot[3:])
                current_minutes = now.hour * 60 + now.minute
                if slot_minutes <= current_minutes:
                    continue
                    
            slots.append(time_slot)
            
        return slots

    def booking_window_dates(self) -> List[datetime.date]:
        today = datetime.today().date()
        start_of_week = today - timedelta(days=today.weekday())
        dates = [start_of_week + timedelta(days=i) for i in range(14)]
        return [date for date in dates if date >= today]

    def available_dates(self, active_bookings: List[Dict[str, str]]) -> List[datetime.date]:
        dates = []
        for date in self.booking_window_dates():
            if self.free_times_for_date(date, active_bookings):
                dates.append(date)
        return dates

    def date_keyboard(
        self,
        page: int = 0,
        active_bookings: Optional[List[Dict[str, str]]] = None,
    ):
        dates = self.booking_window_dates()
        if active_bookings is not None:
            dates = [
                date
                for date in dates
                if self.free_times_for_date(date, active_bookings)
            ]
        formatted = [format_date_with_weekday(date) for date in dates]
        return paginate_buttons(
            formatted,
            target="date",
            page=page,
            buttons_per_row=3,
            rows_per_page=4,
        )
        
    def time_keyboard(
        self,
        selected_date: datetime.date,
        active_bookings: Optional[List[Dict[str, str]]] = None,
        page: int = 0,
    ):
        free_times = self.free_times_for_date(selected_date, active_bookings)
        keyboard = paginate_buttons(
            free_times,
            target="time",
            page=page,
            buttons_per_row=4,
            rows_per_page=5,
        )
        return free_times, keyboard
    