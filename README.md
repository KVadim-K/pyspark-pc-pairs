# PySpark: пары «Продукт – Категория» + продукты без категорий

<div align="center">

![CI](https://github.com/KVadim-K/pyspark-pc-pairs/actions/workflows/ci.yml/badge.svg)
![Python](https://img.shields.io/badge/python-3.10%20|%203.11-blue.svg)
![PySpark](https://img.shields.io/badge/pyspark-3.4+-orange.svg)
![Java](https://img.shields.io/badge/java-17-red.svg) 
![pytest](https://img.shields.io/badge/pytest-7+-green.svg)
![License](https://img.shields.io/badge/license-MIT-green.svg)

</div>

Практическая реализация на PySpark для работы с многие-ко-многим связями: метод возвращает **все пары** «Имя продукта – Имя категории» и **показывает продукты без категорий** (`category_name = NULL`). Демонстрирует работу с LEFT JOIN, дедупликацией и гибкой настройкой имён колонок.

---

## 📋 Содержание

- [Что делает решение](#-что-делает-решение)
- [Установка](#️-установка)
- [Запуск тестов и демо](#-запуск-тестов-и-демо)
- [.env (опционально)](#-env-опционально)
- [Структура проекта](#-структура-проекта)
- [Что проверяют тесты](#-что-проверяют-тесты)
- [Требования](#-требования)
- [Лицензия](#-лицензия)

---


## 🎯 Что делает решение

* **Вход**: три датафрейма — `products`, `categories`, `product_categories`.
* **Логика**:
  * `LEFT JOIN` от продуктов к связям, затем к категориям — так **не теряются** продукты без связей.
  * `dropDuplicates` — страховка от дублей в таблице связей.
* **Выход**: датафрейм с колонками:
  * `product_name : string`
  * `category_name : string | null`

Схема на пальцах:
```
products p                    product_categories pc                    categories c
-----------                   --------------------                    ------------
 id | name   <--(LEFT JOIN)--- product_id | category_id ---JOIN----->  id | name
```

## ⚙️ Установка

### 1️⃣ Виртуальное окружение
```bash
python -m venv .venv

# Windows PowerShell:
.\.venv\Scripts\Activate.ps1

# macOS/Linux:
source .venv/bin/activate

python -m pip install --upgrade pip
```

### 2️⃣ Зависимости
```bash
pip install -r requirements.txt
# или
pip install pyspark pytest
```

## 🚀 Запуск тестов и демо

> ⚠️ **Windows + Spark требует Java 17.** Поставьте [Temurin JDK 17](https://adoptium.net/) и укажите `JAVA_HOME`.

### 💻 Windows PowerShell
```powershell
# из корня репозитория
. .\.venv\Scripts\Activate.ps1

# разово на текущую консольную сессию
$env:JAVA_HOME="C:\Program Files\Eclipse Adoptium\jdk-17.0.16.8-hotspot"
$env:PYSPARK_PYTHON="$pwd\.venv\Scripts\python.exe"
$env:PYSPARK_DRIVER_PYTHON=$env:PYSPARK_PYTHON
$env:PYTHONPATH="src"
$env:SPARK_LOCAL_IP="127.0.0.1"

# Тесты
pytest -q

# Демо
python demo\demo.py
```

### 🍎 macOS / 🐧 Linux
```bash
source .venv/bin/activate

export JAVA_HOME=$(/usr/libexec/java_home -v 17)  # macOS, или путь к JDK 17 на Linux
export PYSPARK_PYTHON="$(pwd)/.venv/bin/python"
export PYSPARK_DRIVER_PYTHON="$PYSPARK_PYTHON"
export PYTHONPATH=src
export SPARK_LOCAL_IP=127.0.0.1

pytest -q
python demo/demo.py
```

**Ожидаемый вывод (пример):**
```
+------------+-------------+
|product_name|category_name|
+------------+-------------+
|Блендер     |Электроника  |
|Гриль       |null         |
|Тостер      |Кухня        |
|Тостер      |Электроника  |
|Чайник      |Кухня        |
+------------+-------------+
```

## 🔧 .env (опционально)

Можно хранить переменные в `.env` (в корне), а IDE будет их подхватывать:
```
JAVA_HOME=C:\Program Files\Eclipse Adoptium\jdk-17.0.16.8-hotspot
PYTHONPATH=src
SPARK_LOCAL_IP=127.0.0.1
```

## 📁 Структура проекта
```
pyspark-pc-pairs/
├─ src/
│  └─ pc_pairs.py
├─ demo/
│  └─ demo.py
├─ tests/
│  └─ test_pc_pairs.py
├─ requirements.txt
├─ pyproject.toml
├─ .env              # (опционально) JAVA_HOME, PYTHONPATH, SPARK_LOCAL_IP
└─ .github/workflows/ci.yml
```

---

## ✅ Что проверяют тесты

* ✔️ Функция корректно собирает пары при наличии/отсутствии связей и с нестандартными именами колонок.
* ✔️ Нет «потери» продуктов без категорий — по ним `category_name = NULL`.
* ✔️ Дубли в таблице связей не приводят к дублированию строк результата (за счёт `dropDuplicates`).

---
## 📦 Требования

<div align="center">

| Компонент | Версия |
|-----------|--------|
| ![Python](https://img.shields.io/badge/python-3.10%20%7C%203.11-blue.svg) | 3.10–3.11 (рекомендуется 3.11) | 
| ![Java](https://img.shields.io/badge/java-17-red.svg) | 17 (Temurin/OpenJDK) | 
| ![PySpark](https://img.shields.io/badge/pyspark-3.4+-orange.svg) | 3.4+ | 
| ![pytest](https://img.shields.io/badge/pytest-7+-green.svg) | 7+ |

</div>

---

## 📄 Лицензия

MIT License

---

<div align="center">

⭐ **Понравился проект?** Поставь звезду! ⭐

Made with ❤️ and ☕

</div>