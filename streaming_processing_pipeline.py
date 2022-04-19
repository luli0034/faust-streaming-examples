""" Streaming Processing Example.
    [Topic] <-(consume)- [processer] -(sink)-> [new_topic]
1) We create a fake database to simulate the external data
2) Continusly listening the topic to get the event
2) Join the external infomation then sink the processed data into new topic

Usage:
1. Open a new terminal to check the faust App
   $ python streaming_processing_pipeline.py worker -l info
2. Then in new ternal, send the kafka message to the topic
   $ python streaming_processing_pipeline.py produce {id} {user_name}

Example:
Type the commands as the following:
$ [TERMINAL 1] python streaming_processing_pipeline.py worker -l info
$ [TERMINAL 2] python streaming_processing_pipeline.py produce 1 Luli
$ [TERMINAL 2] python streaming_processing_pipeline.py produce 2 Tracy
$ [TERMINAL 2] python streaming_processing_pipeline.py produce 88 Error

Then you will see logs:
[2022-04-19 08:15:56,887] [7698] [WARNING] [SOURCE] - <User: id='1', user_name='Luli'> 
[2022-04-19 08:15:56,892] [7698] [WARNING] [TARGET] - <UserJoin: id='1', user_name='Luli', info='data_for_1'> 
[2022-04-19 08:17:59,739] [7698] [WARNING] [SOURCE] - <User: id='2', user_name='Tracy'> 
[2022-04-19 08:17:59,744] [7698] [WARNING] [TARGET] - <UserJoin: id='2', user_name='Tracy', info='data_for_2'> 
[2022-04-19 08:19:12,342] [7698] [WARNING] [SOURCE] - <User: id='88', user_name='Error'> 
[2022-04-19 08:19:12,346] [7698] [WARNING] [TARGET] - <UserJoin: id='88', user_name='Error', info='User not exist'> 
"""
import faust 
from faust.cli import argument
"""
Create a fake database
"""
class Database:
    def __init__(self):
        self.db = {
            1: 'data_for_1',
            2: 'data_for_2',
            3: 'data_for_3',
            4: 'data_for_4',
            5: 'data_for_5',
            6: 'data_for_6',
            7: 'data_for_7',
            8: 'data_for_8',
            9: 'data_for_9',
            10: 'data_for_10'
        }
    def query(self, key):
        try:
            return self.db[int(key)]
        except:
            return 'User not exist'
           
"""
Define data model for type valid 
"""
# Define a source model
class User(faust.Record):
    id: int
    user_name: str

# Define a target model
class UserJoin(faust.Record):
    id: int
    user_name: str
    info: str

"""
Define faust main app
"""
app = faust.App(
    'streaming-processing-pipeline',
    topic_partitions=4,
    version=2,
)

"""
Create two topics: [source_topic, target_topic]
Establish database connection
"""
source_topic = app.topic('src', key_type=str, value_type=User)
target_topic = app.topic('tgt', key_type=str, value_type=UserJoin)
db = Database()

"""
Create an agent to listen on the events of source topic
"""
@app.agent(source_topic, sink=[target_topic])
async def SourceAgent(stream):
    async for value in stream:
        print(f'[SOURCE] - {value}')
        processed_data = UserJoin(id=value.id, user_name=value.user_name, info=db.query(value.id))
        yield processed_data

"""
Create an agent to listen on the events of target topic
"""
@app.agent(target_topic)
async def TargetAgent(stream):
    async for value in stream:
        print(f'[TARGET] - {value}')

"""
Define a cli command to send message to source topic
Usage: $ python streaming_processing_pipeline.py produce {id} {user_name}
"""
@app.command(
    argument('id'),
    argument('user_name'),
)
async def produce(self, id: int, user_name: str):
    await source_topic.send(value=User(id=id, user_name=user_name))

if __name__ == "__main__":
    app.main()