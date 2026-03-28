#!/bin/bash
DB_PATH="/opt/activate/site.db"

# Удаляем использованные активации старше 90 дней
sqlite3 "$DB_PATH" "DELETE FROM activation_links WHERE status='used' AND created_at < datetime('now', '-90 days');"

# Удаляем оплаченные заказы старше 90 дней
sqlite3 "$DB_PATH" "DELETE FROM orders WHERE status='paid' AND paid_at < datetime('now', '-90 days');"

# Удаляем старые вебхуки (храним только последние 30 дней)
sqlite3 "$DB_PATH" "DELETE FROM platega_webhooks WHERE received_at < datetime('now', '-30 days');"

# Оптимизируем базу после удаления
sqlite3 "$DB_PATH" "VACUUM;"

echo "$(date): Cleanup completed" >> /opt/activate/cleanup.log
