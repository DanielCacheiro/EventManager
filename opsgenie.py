import logging
import opsgenie_sdk

# Configuración de OpsGenie
configuration = opsgenie_sdk.Configuration()
configuration.api_key['Authorization'] = 'API_KEY'

# Función para enviar una alerta en OpsGenie
def send_alert(message, priority, tags, department):
    alert = configuration.create_alert(
        message=message,
        priority=priority,
        tags=tags,
        team_id=department
    )
    if alert:
        logging.info('Alerta enviada correctamente')
    else:
        logging.error('Error al enviar la alerta')

# Función para monitorizar el sistema
def monitor_system(event):
    # Verificar si el evento es importante
    if event['importance'] > 0.9:
        # Enviar una alerta en OpsGenie
        message = f'Evento importante: {event["description"]}'
        priority = 'P1'
        tags = ['evento_importante', 'departamento_1']
        send_alert(message, priority, tags)
