# Налаштування SMTP для Airflow Email Notifications

## Опція 1: Локальний SMTP-сервер через Python

### Крок 1: Запустіть локальний SMTP-сервер

Створіть простий SMTP-сервер на Python, який буде виводити всі листи в консоль:

```bash
# На хост-машині (не в контейнері)
python3 -m smtpd -n -c DebuggingServer localhost:1025
```

Або використайте більш розширений варіант з логуванням:

```python
# smtp_server.py - створіть цей файл на хост-машині
import smtpd
import asyncore

server = smtpd.DebuggingServer(('0.0.0.0', 1025), None)

print("SMTP Debug Server running on port 1025")
print("All emails will be printed to console")
asyncore.loop()
```

Запустіть: `python3 smtp_server.py`

### Крок 2: Налаштуйте Airflow

Оновіть `airflow.cfg`:

```ini
[smtp]
smtp_host = host.docker.internal  # Для Docker Desktop на Mac/Windows
# або
smtp_host = 172.17.0.1  # Docker bridge IP (може відрізнятися)
smtp_starttls = False
smtp_ssl = False
smtp_user = 
smtp_password = 
smtp_port = 1025
smtp_mail_from = airflow@localhost
```

### Крок 3: Перезапустіть Airflow

```bash
docker-compose restart airflow-webserver airflow-scheduler
```

## Опція 2: Mailtrap (рекомендовано для тестування)

Mailtrap - це сервіс для тестування email без реальної відправки.

### Крок 1: Зареєструйтеся на mailtrap.io

Отримайте credentials з вашого inbox.

### Крок 2: Налаштуйте Airflow

```ini
[smtp]
smtp_host = smtp.mailtrap.io
smtp_starttls = True
smtp_ssl = False
smtp_user = ваш_username
smtp_password = ваш_password
smtp_port = 2525
smtp_mail_from = airflow@example.com
```

## Опція 3: MailHog (Docker контейнер)

### Крок 1: Додайте MailHog до docker-compose.yaml

```yaml
  mailhog:
    image: mailhog/mailhog:latest
    container_name: mailhog
    ports:
      - "1025:1025"  # SMTP port
      - "8025:8025"  # Web UI port
    networks:
      - data_network
```

### Крок 2: Оновіть Airflow config

```ini
[smtp]
smtp_host = mailhog
smtp_starttls = False
smtp_ssl = False
smtp_user = 
smtp_password = 
smtp_port = 1025
smtp_mail_from = airflow@example.com
```

### Крок 3: Перезапустіть сервіси

```bash
docker-compose up -d
```

Веб-інтерфейс MailHog буде доступний на: http://localhost:8025

## Опція 4: Gmail SMTP (для реальної відправки)

### Крок 1: Налаштуйте App Password

1. Увійдіть в Google Account
2. Увімкніть 2-Step Verification
3. Створіть App Password для "Mail"

### Крок 2: Налаштуйте Airflow

```ini
[smtp]
smtp_host = smtp.gmail.com
smtp_starttls = True
smtp_ssl = False
smtp_user = ваш_email@gmail.com
smtp_password = ваш_app_password  # Не звичайний пароль!
smtp_port = 587
smtp_mail_from = ваш_email@gmail.com
```

## Перевірка налаштувань

### Тест через Python:

```python
from airflow.utils.email import send_email

send_email(
    to='test@example.com',
    subject='Test Email',
    html_content='<h1>Test</h1><p>This is a test email</p>',
    mime_subtype='html'
)
```

### Тест через Airflow CLI:

```bash
docker exec airflow_scheduler airflow tasks test process_iris notify_email 2025-04-22
```

### Перевірка email в MailHog:

1. **Веб-інтерфейс**: Відкрийте http://localhost:8025 в браузері
2. **API перевірка**:
```bash
# Перевірити кількість email
curl -s http://localhost:8025/api/v2/messages | python3 -m json.tool | grep -A 5 "total"

# Показати останні email
curl -s http://localhost:8025/api/v2/messages | python3 -c "import sys, json; data = json.load(sys.stdin); [print(f\"To: {item['To'][0]['Mailbox']}@{item['To'][0]['Domain']}, Subject: {item.get('Content', {}).get('Headers', {}).get('Subject', ['N/A'])[0]}\") for item in data['items'][:5]]"
```

3. **Очистити MailHog** (якщо потрібно):
```bash
curl -X DELETE http://localhost:8025/api/v1/messages
```

## Рекомендації

- **Для розробки/тестування**: Використовуйте MailHog або локальний SMTP-сервер
- **Для staging**: Mailtrap
- **Для production**: Реальний SMTP (Gmail, SendGrid, AWS SES тощо)

## Налаштування Variable для email адреси

Встановіть змінну в Airflow UI або через CLI:

```bash
# Через CLI
docker exec airflow_scheduler airflow variables set notify_email your-email@example.com

# Через UI: Admin → Variables → Create
```

