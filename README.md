Проект представлен в виде сервиса по обработке заказов
Технологии: PostgreSQL, Redis, Kafka
API endpoints: 
    /GET /order/:order_id. Ответ - Json с данными о заказе
    / и /static/* - html, css, js файлы
Механизм развертывания: docker-compose up --build 
"Внешний" адрес кафки - localhost:9094