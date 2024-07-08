# Металлургический завод и его поставщики

## Описание проекта

Нужно создать хранилище данных для металлургического завода и его поставщиков.



## Инструменты

- **python**
- **faker**
- sqlalchemy.**create_engine**
- sqlalchemy.**Column**
- sqlalchemy.**Integer**
- sqlalchemy.**String**
- sqlalchemy.**Float**
- sqlalchemy.**Text**
- sqlalchemy.**Enum**
- sqlalchemy.**ForeignKey**
- sqlalchemy.**DateTime**
- sqlalchemy.ext.declarative.**declarative_base**
- sqlalchemy.orm.**sessionmaker**

## Общий вывод

Были сгенерированы данные при помощи библиотеки Faker. Созданы слои: staging, ods, reference, integration, dds, data mart. Написаны процедуры для их наполнения.
