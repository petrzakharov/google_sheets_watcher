1. Создайте credentials и загрузите полученный json в корень проекта
```python 
https://developers.google.com/workspace/guides/create-credentials?hl=en
```
2. Создайте .env файл по примеру .env_example
3. Откройте доступ к документу на сервисный аккаунта из пункта 1
4. Измените переменную SPREADSHEET_ID на ваш ID документа
5. Запустите docker-compose
```python
docker-compose up
```