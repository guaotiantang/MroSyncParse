import datetime
from typing import Any

from async_generator import asynccontextmanager
from tortoise import Model, fields, transactions, Tortoise

from Config import MysqlInfo
from tortoise import Model, fields





class MroTask(Model):
    # Mro子任务管理
    task_id = fields.IntField(pk=True)
    main_zip = fields.CharField(max_length=255)
    sub_zip_path = fields.CharField(max_length=255)
    xml_name = fields.CharField(max_length=255)
    task_status = fields.CharField(max_length=255)
    uptime = fields.DatetimeField()
    ftp_name = fields.CharField(max_length=255)

    def __init__(self, **kwargs: Any):
        super().__init__(**kwargs)
        self._db_initialized = False

    async def connect_to_db(self, user, passwd, host, port):
        if not self._db_initialized:
            await Tortoise.init(
                db_url=f'mysql://{user}:{passwd}@{host}:{port}/mroparse',
                modules={'models': ['__main__']}
            )
            await Tortoise.generate_schemas()
            self._db_initialized = True

    @asynccontextmanager
    async def transaction_context(self):
        async with transactions.in_transaction():
            yield

    async def tasks_add(self, task, ftp_name):
        async with self.transaction_context():
            # 检查数据库中是否存在相同的数据
            existing_task = await MroTask.filter(
                main_zip=task['main_zip'], sub_zip_path=task['sub_zip_path'], ftp_name=ftp_name
            ).first()
            # 如果不存在相同的数据，则将其添加到数据库中
            if not existing_task:
                await MroTask.create(
                    main_zip=task['main_zip'],
                    sub_zip_path=task['sub_zip_path'],
                    xml_name="unparse",
                    task_status="unparse",
                    uptime=datetime.datetime.now(),
                    ftp_name=ftp_name
                )

    async def tasks_get(self, task_num, ftp_name):
        async with self.transaction_context():
            raw_tasks = await MroTask.raw(
                f"SELECT * FROM mrotask WHERE task_status='unparse' AND ftp_name='{ftp_name}' LIMIT {task_num} FOR UPDATE"
            )
            tasks = [MroTask(**task) for task in raw_tasks]

            for task in tasks:
                task.task_status = 'parsing'
                task.uptime = datetime.datetime.now()
                await task.save()
        return tasks

    async def tasks_update(self, task_id, status):
        async with self.transaction_context():
            task = await MroTask.get(task_id=task_id)
            task.task_status = status
            task.uptime = datetime.datetime.now()
            await task.save()
