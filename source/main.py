# Интерфейс программы
import PySimpleGUI as sg

# Обработка данных
from data_processing import load_data, prepare_tfidf_matrix, batch_search, save_to_excel

# Настройка темы для PySimpleGUI
sg.theme('DarkBlue')

# Интерфейс
layout = [
    [sg.Button('Открыть Excel файл с товарами', key='load_data')],
    [sg.Button('Открыть Excel файл с заявками', key='load_query')],
    [sg.Button('Выгрузить данные в Excel файл', key='extract_result')],
    [sg.Text('Количество релевантных позиций:'), sg.InputText('5', size=(5, 1), key='top_n')],
    [sg.Text('Статус:', size=(10, 1)), sg.Text('', size=(40, 1), key='status')]
]

# Создание окна
window = sg.Window('Поиск товаров', layout)

# Инициализация переменных
inventory_df = None
query_df = None
vectorizer = None
tfidf_matrix = None

# Главный цикл обработки событий
while True:
    event, values = window.read()

    # Пользователь вышел из приложения
    if event == sg.WIN_CLOSED:
        break
    # Загрузка данных о товарах
    if event == 'load_data':
        file_path = sg.popup_get_file('Выберите Excel файл с товарами', file_types=(("Excel Files", "*.xlsx"),))
        if file_path:
            try:
                inventory_df = load_data(file_path, usecols=["Номенклатура", "ТоварПроизводителя","Оформлено",'ОформленоЧастично','БезОформления','Код','ОсновнойАссортимент'])
                vectorizer, tfidf_matrix = prepare_tfidf_matrix(inventory_df)
                window['load_data'].update(button_color=('green'), text='Данные о товаре загружены!')
                window['status'].update('Данные о товарах загружены!', text_color='orange')
            except Exception as e:
                window['status'].update(f'Ошибка: {e}', text_color='red')

    # Загрузка данных с запросами
    if event == 'load_query':
        file_path = sg.popup_get_file('Выберите Excel файл с запросами', file_types=(("Excel Files", "*.xlsx"),))
        if file_path:
            try:
                query_df = load_data(file_path, usecols=["Номенклатура"])
                window['load_query'].update(button_color=('green'), text='Запросы загружены!')
                window['status'].update('Данные с запросами загружены!', text_color='orange')
            except Exception as e:
                window['status'].update(f'Ошибка: {e}', text_color='red')

    # Выгрузка данных в Excel файл
    if event == 'extract_result':
        if inventory_df is not None and query_df is not None:
            try:
                # Получаем значение top_n от пользователя
                try:
                    top_n = int(values['top_n'])
                    if top_n <= 0:
                        raise ValueError("Количество позиций должно быть положительным.")
                except Exception:
                    sg.popup('Некорректное значение для количества позиций!', title='Ошибка')
                    continue

                search_results_df = batch_search(
                    query_df['Номенклатура'], vectorizer, tfidf_matrix, inventory_df,
                    top_n=top_n, batch_size=100, max_workers=4
                )
                if not search_results_df.empty:
                    save_path = sg.popup_get_file(
                        'Сохранить файл',
                        save_as=True,
                        no_titlebar=True,
                        file_types=(('Excel Files', '*.xlsx'),)
                    )
                    if not save_path:
                        sg.popup('Сохранение отменено пользователем.', title='Отмена')
                        continue

                    if not save_path.endswith('.xlsx'):
                        save_path += '.xlsx'

                    # Переставляем столбцы в нужном порядке
                    final_df = search_results_df[
                        ['Запрос','Номенклатура и код товара', 'Сходство','ТоварПроизводителя','Оформлено', 'ОформленоЧастично',
                         'БезОформления','ОсновнойАссортимент']]

                    save_to_excel(final_df, save_path)
                    sg.popup('Результаты успешно сохранены!', title='Успех')
                    window['load_query'].update(button_color=('white'), text='Открыть Excel файл с заявками')
                else:
                    sg.popup('Нет найденных товаров для сохранения', title='Результат поиска')
            except Exception as e:
                print(e)
                sg.popup(f'Ошибка при обработке данных: {e}', title='Ошибка')
        else:
            sg.popup('Сначала откройте Excel файлы с товарами и запросами!', title='Внимание')

# Закрытие окна
window.close()