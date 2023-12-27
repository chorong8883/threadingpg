# threadingpg
Control PostgreSQL using thread(s).

## Initialize Controller  
```python  
import threadingpg
controller = threadingpg.Controller()
controller.connect(dbname='database_name', user='user_name', password='password', port=5432)
# ...
controller.close()
```

## Initialize Pool  
```python  
import threadingpg
controller = threadingpg.Pool(dbname='database_name', user='user_name', password='password', port=5432)
# ...
controller.close()
```

## Table and Row
### Table
```python  
import threadingpg
class MyTable(threadingpg.Table):
    table_name="mytable"
    index = threadingpg.Column(data_type=threadingpg.types.serial)
    name = threadingpg.Column(data_type=threadingpg.types.varchar())
# or 
class MyTable(threadingpg.Table):
    def __init__(self) -> None:
        self.index = threadingpg.Column(data_type=threadingpg.types.serial)
        self.name = threadingpg.Column(data_type=threadingpg.types.varchar())
        super().__init__("mytable") # important position
```
#### Create/Drop Table
```python  
mytable = MyTable()
controller.create_table(mytable)
controller.drop_table(mytable)
```

### Row
```python
class MyRow(threadingpg.Row):
    def __init__(self, name:str=None) -> None:
        self.name = name
```
#### Insert Row
```python
mytable = MyTable()
myrow = MyRow("my_row")
controller.insert_row(mytable, myrow)
# or
controller.insert_dict(mytable, {"name":"my_row"})
```
#### Select Row
```python
mytable = MyTable()
column_name_list, rows = controller.select(mytable)
for row in rows:
    myrow = MyRow()
    myrow.set_data(column_name_list, row)
    print(f"output: {myrow.name}") # output: my_row
```
#### Update Row
```python
mytable = MyTable()
myrow = MyRow("update_my_row")
condition_equal_0 = threadingpg.condition.Equal(mytable.index, 0)
controller.update_row(mytable, myrow, condition_equal_0)
```
#### Delete Row
```python
mytable = MyTable()
delete_condition = threadingpg.condition.Equal(mytable.index, 5)
controller.delete_row(mytable, delete_condition)
```

### Conditions
#### Where 
```python
mytable = MyTable()
condition_equal_1 = threadingpg.condition.Equal(mytable.index, 1)
condition_equal_2 = threadingpg.condition.Equal(mytable.index, 2)
condition_equal_3 = threadingpg.condition.Equal(mytable.index, 3)
conditions = threadingpg.condition.Or(condition_equal_1, condition_equal_2, condition_equal_3)
column_name_list, rows = controller.select(mytable, where=conditions)
```
#### OrderBy
```python
mytable = MyTable()
orderby_index = threadingpg.condition.OrderBy(mytable.index)
orderby_name = threadingpg.condition.OrderBy(mytable.name, True)
orderby_conditions = threadingpg.condition.And(orderby_index, orderby_name)
column_name_list, rows = controller.select(mytable, order_by=orderby_conditions)
```

## Trigger
Need delay each function.
```python
mytable = MyTable()
channel_name = "mych"
trigger_name = "mytr"
function_name = "myfn"

listner = threadingpg.TriggerListner()
# implement 'notify = listner.notify_queue.get()'

listner.connect(dbname=dbname, user=user, password=password, port=5432)
listner.create_function(function_name, channel_name)
listner.create_trigger(mytable, trigger_name, function_name)

listner.start_listening()
listner.listen_channel(channel_name)
# ...
listner.unlisten_channel(channel_name)
listner.stop_listening()
```