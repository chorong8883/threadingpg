# threadingpg
Control PostgreSQL using thread(s).

## Initialize Connector  
```python  
import threadingpg
connector = threadingpg.Connector(dbname='database_name', user='user_name', password='password', port=5432)
# ...
connector.close()
```

## Drop, Create Table
```python  
mytable = MyTable()
connector.drop_table(mytable)
connector.create_table(mytable)
```

## Create Table and Row Class
### Table Class
```python  
import threadingpg
from threadingpg import datatype

class MyTable(threadingpg.data.Table):
    table_name="mytable"
    index = threadingpg.data.Column(data_type=datatype.serial)
    name = threadingpg.data.Column(data_type=datatype.varchar())
# or 
class MyTable(threadingpg.data.Table):
    def __init__(self) -> None:
        self.index = threadingpg.data.Column(data_type=datatype.serial)
        self.name = threadingpg.data.Column(data_type=datatype.varchar())
        super().__init__("mytable") # important position
```

### Row Class
equal name of columns.
```python
class MyRow(threadingpg.data.Row):
    def __init__(self,
                 name:str=None) -> None:
        self.name = name
```

## Insert
```python
mytable = MyTable()
myrow = MyRow("my_row")
connector.insert_row(mytable, myrow)
# or
connector.insert_dict(mytable, {"name":"my_row"})
```

## Select
```python
mytable = MyTable()
column_name_list, rows = connector.select(mytable)
for row in rows:
    myrow = MyRow()
    myrow.set_data(column_name_list, row)
    print(f"output: {myrow.name}") # output: my_row
```

### Condition - Where 
```python
mytable = MyTable()
condition_equal_1 = threadingpg.condition.Equal(mytable.index, 1)
condition_equal_2 = threadingpg.condition.Equal(mytable.index, 2)
condition_equal_3 = threadingpg.condition.Equal(mytable.index, 3)
conditions = threadingpg.condition.Or(condition_equal_1, condition_equal_2, condition_equal_3)
column_name_list, rows = connector.select(mytable, where=conditions)
```
### Condition - OrderBy
```python
mytable = MyTable()
orderby_index = threadingpg.condition.OrderBy(mytable.index)
orderby_name = threadingpg.condition.OrderBy(mytable.name, True)
orderby_conditions = threadingpg.condition.And(orderby_index, orderby_name)
column_name_list, rows = connector.select(mytable, order_by=orderby_conditions)
```

## Update
```python
mytable = MyTable()
myrow = MyRow("update_my_row")
condition_equal_0 = threadingpg.condition.Equal(mytable.index, 0)
connector.update_row(mytable, myrow, condition_equal_0)
```

## Delete
```python
mytable = MyTable()
delete_condition = threadingpg.condition.Equal(mytable.index, 5)
connector.delete_row(mytable, delete_condition)
```

## Simple Trigger
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