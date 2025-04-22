# pgXRay - Инструмент расширенного аудита PostgreSQL

![PostgreSQL](https://img.shields.io/badge/PostgreSQL-336791?style=for-the-badge&logo=postgresql&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)
![License](https://img.shields.io/badge/License-CC_BY_4.0-lightgrey.svg)
![Version](https://img.shields.io/badge/Version-3.1.0-blue.svg)

pgXRay - это мощный инструмент для комплексного аудита и документирования PostgreSQL баз данных. Проект создан для автоматизации процесса аудита структуры базы данных и генерации документации, включая визуальные ER-диаграммы.

## 🚀 Возможности

- **Полный аудит структуры БД**:
  - Таблицы, колонки, типы данных
  - Индексы и ограничения
  - Внешние ключи и связи
  
- **Анализ данных**:
  - Выборка примеров данных из каждой таблицы (до SAMPLE_LIMIT строк)
  - Оценка количества строк и размера таблиц
  
- **Исследование кода**:
  - Полные тексты всех функций
  - Триггеры и их реализация
  
- **Визуализация**:
  - ER-диаграммы в формате DOT и PNG
  - HTML-таблицы в узлах диаграмм
  - XLabel-подписи рёбер для отношений
  
- **Отчетность**:
  - Подробный отчет в формате Markdown
  - Логирование процесса выполнения

## 📋 Требования

- Python 3.9+
- psycopg2-binary >= 2.9
- graphviz >= 0.20
- Утилита `dot` (часть пакета Graphviz)

## ⚙️ Установка

```bash
# Клонирование репозитория
git clone https://github.com/T-6891/pgXRay.git
cd pgXRay

# Настройка виртуального окружения Python
python -m venv venv

# Активация виртуального окружения
# На Windows
# venv\Scripts\activate
# На macOS/Linux
source venv/bin/activate

# Установка зависимостей
pip install -r requirements.txt

# Установка Graphviz (Linux)
sudo apt install graphviz

# Установка Graphviz (macOS)
brew install graphviz

# Установка Graphviz (Windows)
# Скачайте и установите с https://graphviz.org/download/
```

## 🔧 Использование

### Базовый аудит

```bash
python pgXRay.py --conn "postgresql://user:password@host:port/database" --md "audit_report.md"
```

### Полный набор параметров

```bash
python pgXRay.py --conn "postgresql://user:password@host:port/database" \
                 --md "audit_report.md" \
                 --dot "er_diagram.dot" \
                 --png "er_diagram.png"
```

### Параметры командной строки

| Параметр | Описание | По умолчанию |
|---------|------------|---------|
| `--conn` | Строка подключения к PostgreSQL | (обязательный) |
| `--md` | Путь для сохранения Markdown-отчета | `audit_report.md` |
| `--dot` | Путь для сохранения DOT-файла диаграммы | `er_diagram.dot` |
| `--png` | Путь для сохранения PNG-диаграммы | `er_diagram.png` |

## 📊 Результаты аудита

В результате выполнения скрипта будут созданы:

1. **ER-диаграмма** в форматах DOT и PNG с визуализацией связей между таблицами
2. **Markdown-отчет**, включающий:
   - Общую информацию о базе данных (версия PostgreSQL, размер БД)
   - Структуру таблиц с типами данных
   - Примеры данных из каждой таблицы
   - Полные тексты функций
   - Определения триггеров

## 🛠️ Конфигурация

Основные настройки находятся в верхней части скрипта:

```python
# =====================
# Configuration
# =====================
SAMPLE_LIMIT      = 10                  # количество строк для выборки
DOT_FILE          = 'er_diagram.dot'    # имя .dot-файла
PNG_FILE          = 'er_diagram.png'    # имя .png-файла
DEFAULT_MD_REPORT = 'audit_report.md'
# =====================
```

## 🤝 Вклад в развитие

Вклады приветствуются! Пожалуйста, не стесняйтесь открывать issues или pull requests.

1. Форкните репозиторий
2. Создайте ветку с вашей функциональностью (`git checkout -b feature/amazing-feature`)
3. Зафиксируйте изменения (`git commit -m 'Add some amazing feature'`)
4. Отправьте ветку (`git push origin feature/amazing-feature`)
5. Откройте Pull Request

## 📄 Лицензия

Проект распространяется под лицензией Creative Commons Attribution 4.0 International (CC BY 4.0).
Подробная информация: [https://creativecommons.org/licenses/by/4.0/](https://creativecommons.org/licenses/by/4.0/)

## 👤 Автор

**Владимир Смельницкий**  
E-mail: master@t-brain.ru

## 📌 Дорожная карта

- [ ] Анализ представлений (views) и материализованных представлений
- [ ] Экспорт в формате HTML
- [ ] Интерактивные диаграммы
- [ ] Сравнение структур баз данных
- [ ] Оптимизация для больших баз данных
