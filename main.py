# pip install pandas mysql-connector-python psycopg2-binary

import pandas as pd

class SQLTable:
    def __init__(self, db_type, db_config, table_name):
        """
        Инициализация. Выбираем СУБД: 'mysql' или 'postgresql'.
        """
        self.db_type = db_type.lower()
        self.table_name = table_name
        self.db_config = db_config

        # Подрубаемся к нужной базе в зависимости от выбора
        if self.db_type == "mysql":
            import mysql.connector
            self.connection = mysql.connector.connect(**db_config)
            self.cursor = self.connection.cursor(dictionary=True)
        elif self.db_type in ["postgresql", "postgres"]:
            import psycopg2
            from psycopg2.extras import RealDictCursor
            self.connection = psycopg2.connect(**db_config)
            # RealDictCursor чтобы Postgres возвращал данные как словари (аналог dictionary=True)
            self.cursor = self.connection.cursor(cursor_factory=RealDictCursor)
        else:
            raise ValueError("Бро, поддерживаются только 'mysql' или 'postgresql'")

    # --- ВНУТРЕННИЕ ПОМОЩНИКИ ---

    def _build_where(self, filters):
        """
        Скрытый метод для сборки WHERE. 
        Берет словарь {'age': 25, 'status': 'active'} и делает из него 
        строку "WHERE age = %s AND status = %s" и список значений.
        Это спасет нас от SQL-инъекций и избавит от хардкода.
        """
        if not filters:
            return "", []
        
        conditions = []
        values = []
        for key, value in filters.items():
            conditions.append(f"{key} = %s")
            values.append(value)
            
        where_clause = "WHERE " + " AND ".join(conditions)
        return where_clause, values

    # --- БАЗОВЫЕ ЗАПРОСЫ С ФИЛЬТРАЦИЕЙ ---

    def select(self, columns="*", filters=None, order_by=None, ascending=True):
        """
        Универсальный селект. Заменяет кучу мелких методов.
        """
        where_clause, params = self._build_where(filters)
        
        order_clause = ""
        if order_by:
            direction = "ASC" if ascending else "DESC"
            order_clause = f"ORDER BY {order_by} {direction}"

        query = f"SELECT {columns} FROM {self.table_name} {where_clause} {order_clause}"
        self.cursor.execute(query, params)
        return self.cursor.fetchall()

    def delete(self, filters):
        """
        Удаление с фильтром по любым условиям.
        """
        if not filters:
            raise ValueError("Удаление всей таблицы без WHERE опасно. Если нужно грохнуть все - используй drop_table.")
            
        where_clause, params = self._build_where(filters)
        query = f"DELETE FROM {self.table_name} {where_clause}"
        self.cursor.execute(query, params)
        self.connection.commit()
        print(f"Строки удалены. Условие: {filters}")

    # --- СЛОЖНЫЕ ЗАПРОСЫ (JOIN / UNION) ---

    def join(self, other_table, on_condition, join_type="INNER", columns="*"):
        """
        Склеиваем две таблицы.
        join_type может быть: INNER, LEFT, RIGHT, FULL
        on_condition: строка с условием, например "table1.id = table2.user_id"
        """
        query = f"""
            SELECT {columns} 
            FROM {self.table_name} 
            {join_type.upper()} JOIN {other_table} 
            ON {on_condition}
        """
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def union(self, other_table, columns="*", union_all=False):
        """
        Объединение результатов. Если union_all=True, дубликаты не удаляются (работает быстрее).
        """
        mode = "UNION ALL" if union_all else "UNION"
        query = f"""
            SELECT {columns} FROM {self.table_name}
            {mode}
            SELECT {columns} FROM {other_table}
        """
        self.cursor.execute(query)
        return self.cursor.fetchall()

    # --- РАБОТА СО СТРУКТУРОЙ ---

    def get_structure(self):
        """
        Получение структуры таблицы.
        """
        if self.db_type == "mysql":
            self.cursor.execute(f"DESCRIBE {self.table_name}")
        else:
            # Для Postgres лезем в системную таблицу
            query = """
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = %s
            """
            self.cursor.execute(query, (self.table_name,))
        return self.cursor.fetchall()

    def drop_table(self):
        self.cursor.execute(f"DROP TABLE IF EXISTS {self.table_name}")
        self.connection.commit()
        print(f"Таблица {self.table_name} стерта с лица земли.")

    def add_column(self, col_name, col_type="VARCHAR(255)"):
        self.cursor.execute(f"ALTER TABLE {self.table_name} ADD COLUMN {col_name} {col_type}")
        self.connection.commit()

    def drop_column(self, col_name):
        self.cursor.execute(f"ALTER TABLE {self.table_name} DROP COLUMN {col_name}")
        self.connection.commit()

    # --- ЭКСПОРТ И ИМПОРТ ---

    def export_to_csv(self, file_path):
        """
        Экспорт через pandas.
        """
        query = f"SELECT * FROM {self.table_name}"
        df = pd.read_sql(query, self.connection)
        df.to_csv(file_path, index=False)
        print(f"Данные успешно улетели в {file_path}")

    def import_from_csv(self, file_path):
        """
        Импорт данных из CSV файла в таблицу.
        """
        df = pd.read_csv(file_path)
        if df.empty:
            print("CSV файл пуст, загружать нечего.")
            return

        cols = ", ".join(df.columns)
        placeholders = ", ".join(["%s"] * len(df.columns))
        query = f"INSERT INTO {self.table_name} ({cols}) VALUES ({placeholders})"

        data = [tuple(x) for x in df.values]
        self.cursor.executemany(query, data)
        self.connection.commit()
        print(f"Закинуто {len(data)} строк из {file_path}")

    # --- УПРАВЛЕНИЕ ЖИЗНЕННЫМ ЦИКЛОМ ---

    def close(self):
        """
        Явное закрытие коннекта.
        """
        # Проверяем, открыто ли соединение, прежде чем закрывать
        if self.connection:
            is_open = False
            if hasattr(self.connection, 'closed'):
                is_open = not self.connection.closed # Для psycopg2
            elif hasattr(self.connection, 'is_connected'):
                is_open = self.connection.is_connected() # Для mysql.connector
                
            if is_open:
                self.cursor.close()
                self.connection.close()

    def __del__(self):
        # Подстраховка
        try:
            self.close()
        except Exception:
            pass

# ==========================================
# ПРИМЕР ИСПОЛЬЗОВАНИЯ (Тестовый блок)
# ==========================================
if __name__ == "__main__":
    # Настройки для твоей БД (замени на свои реальные данные)
    # Пример для PostgreSQL:
    DB_CONFIG = {
        "host": "localhost",
        "user": "your_username",
        "password": "your_password",
        "database": "your_database",
        # "port": "5432" # Раскомментируй, если нестандартный порт
    }
    
    # 1. Создаем объект (и сразу подключаемся)
    try:
        # Для тестов укажи нужную базу ('postgresql' или 'mysql') и таблицу (например, 'users')
        # my_table = SQLTable("postgresql", DB_CONFIG, "users")
        print("Класс успешно собран! Раскомментируй код выше и подставь свои данные для реального теста.")
        
        # --- Примеры вызовов (сейчас закомментированы, чтобы не было ошибок без реальной БД) ---
        
        # Вывести структуру
        # print("Структура:", my_table.get_structure())
        
        # Получить всех активных админов
        # admins = my_table.select(
        #     filters={"role": "admin", "status": "active"}, 
        #     order_by="id", 
        #     ascending=False
        # )
        # print(admins)
        
        # Сделать JOIN с таблицей заказов
        # orders_info = my_table.join(
        #     other_table="orders", 
        #     on_condition="users.id = orders.user_id", 
        #     join_type="LEFT", 
        #     columns="users.name, orders.total"
        # )
        
        # Экспорт в CSV
        # my_table.export_to_csv("backup_users.csv")
        
        # Закрываем соединение
        # my_table.close()
        
    except Exception as e:
        print(f"Ошибка при работе: {e}")
