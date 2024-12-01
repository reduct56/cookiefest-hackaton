from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import linear_kernel
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
import numpy as np

from nltk.corpus import stopwords
import nltk

def load_data(file_path, usecols=None):
    """
    Загружает данные из Excel файла с оптимизацией.

    :param file_path: Путь к Excel файлу.
    :param usecols: Список необходимых столбцов.
    :return: DataFrame с данными.
    """
    try:
        return pd.read_excel(file_path, usecols=usecols, engine="openpyxl")
    except Exception as e:
        raise ValueError(f"Ошибка загрузки файла: {e}")


# необходимые данные загружены
nltk.download('stopwords', quiet=True)

def prepare_tfidf_matrix(inventory_df):
    """
    Предварительно готовит матрицу TF-IDF для базы данных.

    :param inventory_df: DataFrame с данными о товарах.
    :return: Объекты TF-IDF векторизатора и матрицы.
    """
    stop_words_russian = stopwords.words("russian")
    vectorizer = TfidfVectorizer(stop_words=stop_words_russian, max_features=50000)
    product_texts = (
        inventory_df['Номенклатура'].astype(str).fillna('') + ' ' +
        inventory_df['ТоварПроизводителя'].astype(str).fillna('')
    )
    tfidf_matrix = vectorizer.fit_transform(product_texts)
    return vectorizer, tfidf_matrix


def search_product(query, vectorizer, tfidf_matrix, inventory_df, top_n=5):
    """
    Выполняет поиск одного запроса и включает данные о статусах оформления.

    :param query: Текст запроса.
    :param vectorizer: TF-IDF векторизатор.
    :param tfidf_matrix: Матрица TF-IDF для базы данных.
    :param inventory_df: DataFrame с данными о товарах.
    :param top_n: Количество лучших совпадений.
    :return: DataFrame с результатами поиска.
    """
    # Обработка данных для поиска
    query_tfidf = vectorizer.transform([query])
    cosine_similarities = linear_kernel(query_tfidf, tfidf_matrix).flatten()

    # Если все сходства равны нулю
    if np.all(cosine_similarities == 0):
        return pd.DataFrame()

    # Индексы товаров с наибольшими сходствами
    similar_indices = cosine_similarities.argsort()[-top_n:][::-1]
    results = inventory_df.iloc[similar_indices].copy()

    # Добавляем столбец с сходством
    results['Сходство'] = cosine_similarities[similar_indices]

    # Добавляем столбец с запросом
    results['Запрос'] = query
    # Обрабатываем пропущенные значения и добавляем необходимые столбцы
    results['Оформлено'] = inventory_df['Оформлено']
    results['ОформленоЧастично'] =inventory_df['ОформленоЧастично']
    results['БезОформления'] = inventory_df['БезОформления']
    results['ТоварПроизводителя']=inventory_df['ТоварПроизводителя']
    results['ОсновнойАссортимент']=inventory_df['ОсновнойАссортимент']

    # Добавляем столбец с кодом товара
    results['Код'] = inventory_df['Код'].iloc[similar_indices].values
    results['Номенклатура и код товара'] = (
            inventory_df['Номенклатура'].astype(str) + " (" +
            results['Код'].astype(str) + ")"
        )

    return results


def batch_search(queries, vectorizer, tfidf_matrix, inventory_df, top_n=5, batch_size=100, max_workers=4):
    """
    Параллельный поиск для батчей запросов.

    :param queries: Список текстов запросов.
    :param vectorizer: TF-IDF векторизатор.
    :param tfidf_matrix: Матрица TF-IDF для базы данных.
    :param inventory_df: DataFrame с данными о товарах.
    :param top_n: Количество лучших совпадений.
    :param batch_size: Размер партии запросов.
    :param max_workers: Количество потоков.
    :return: DataFrame с результатами всех запросов.
    """
    def process_batch(batch):
        batch_results = []
        for query in batch:
            result = search_product(query, vectorizer, tfidf_matrix, inventory_df, top_n=top_n)
            if not result.empty:
                result['Запрос'] = query
                batch_results.append(result)
        return pd.concat(batch_results, ignore_index=True) if batch_results else pd.DataFrame()

    batches = [queries[i:i + batch_size] for i in range(0, len(queries), batch_size)]

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        results = executor.map(process_batch, batches)

    return pd.concat(results, ignore_index=True) if results else pd.DataFrame()



def save_to_excel(results_df, output_file, batch_size=10000):
    """
    Сохраняет результаты поиска в Excel частями.

    :param results_df: DataFrame с результатами поиска.
    :param output_file: Имя выходного файла.
    :param batch_size: Размер одной партии строк.
    """
    try:
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            for start_row in range(0, len(results_df), batch_size):
                batch = results_df.iloc[start_row:start_row + batch_size]
                batch.to_excel(writer, index=False, startrow=0 if start_row == 0 else None, sheet_name="Results")
    except Exception as e:
        raise ValueError(f"Ошибка сохранения файла: {e}")

