from enum import Enum
from kafka import KafkaProducer
import json


# 1. Crear un Enumerado para las Categorías
class EventCategory(Enum):
    MACROECONOMIC = 'Macroeconomic_topic'
    HOLIDAY_CULTURAL = 'Holiday/Cultural_topic'
    SOCIAL_TREND = 'Social Trend_topic'
    WEATHER = 'Weather_topic'
    POLITICAL_REGULATORY = 'Political/Regulatory_topic'
    CONSUMER_EVENT = 'Consumer Event_topic'
    SUPPLY_CHAIN = 'Supply Chain_topic'


# 2. Definir Departamentos Relevantes para Cada Categoría
category_department_map = {
    EventCategory.MACROECONOMIC: ['Finance', 'Sales', 'Strategy'],
    EventCategory.HOLIDAY_CULTURAL: ['Sales', 'Logistics'],
    EventCategory.SOCIAL_TREND: ['Marketing', 'Sales', 'Strategy'],
    EventCategory.WEATHER: ['Operations', 'Logistics'],
    EventCategory.POLITICAL_REGULATORY: ['Legal', 'Finance'],
    EventCategory.CONSUMER_EVENT: ['Marketing', 'Sales', 'Strategy'],
    EventCategory.SUPPLY_CHAIN: ['Logistics', 'Operations']
}

def classify_event(event_data):
    """
    Clasifica el evento según su contenido y asigna la categoría correspondiente.
    """
    # Ejemplo de cómo se podría hacer una clasificación (simplificado)
    if 'inflation' in event_data['description'].lower():
        return EventCategory.MACROECONOMIC
    elif 'holiday' in event_data['description'].lower():
        return EventCategory.HOLIDAY_CULTURAL
    elif 'storm' in event_data['description'].lower():
        return EventCategory.WEATHER
    # Añadir más reglas de clasificación aquí...
    else:
        return None