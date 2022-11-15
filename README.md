# pyrogram_patch

pyrogram_patch is a Python library this is a library that adds middlewares and fsm to pyrogram.

## Installation

Use the package manager [pip](https://pip.pypa.io/en/stable/) to install foobar.

```bash
pip install https://github.com/pashcock/pyrogram_patch.git
```

# Middlewares

## Usage

```python
from pyrogram import Client
from pyrogram_patch import patch

# create client
app = Client("my_account", api_id='API_ID', api_hash='API_HASH')

# patch client
patch_manager = patch(app)

# include middleware
patch_manager.include_middleware(MyMiddleware(*args, **kwargs))

```

## Create middleware

```python
from pyrogram_patch.middlewares.middleware_types import OnUpdateMiddleware
from pyrogram_patch.middlewares import MiddlewareHelper


class MyMiddleware(OnUpdateMiddleware):

    # it can be any value you want

    def __init__(self, *args, **kwargs) -> None:
        self.value = 'my_value'

    # you cannot change the call arguments
    async def __call__(self, update, middleware_helper: MiddlewareHelper):
        # do whatever you want
        # need to return dictionary
        return await middleware_helper.insert_data('my_value_name', self.value)

    # get_data() - use this method to get the data you saved earlier
    # skip_handler() - use this method to skip the handler
    # middleware_helper.state.state - this way you can get the current state
```


## Handle midleware data

```python
@app.on_message(filters.me)
async def my_commands(client, message, my_value_name):
    print(my_value_name)
```
## Middleware types and updates
```text
middleware - middleware which is called if the update is used
outer middleware - middleware that handles everything even if it wasn't caught by the handler
```
events and middlewares
```text
on_message - OnMessageMiddleware
on_inline_query - OnInlineQueryMiddleware
on_user_status - OnUserStatusMiddleware
on_disconnect - OnDisconnectMiddleware
on_edited_message - OnEditedMessageMiddleware
on_deleted_messages - OnDeletedMessagesMiddleware
on_chosen_inline_result - OnChosenInlineResultMiddleware
on_chat_member_updated - OnChatMemberUpdatedMiddleware
on_raw_update - OnRawUpdateMiddleware
on_chat_join_request - OnChatJoinRequestMiddleware
on_callback_query - OnCallbackQueryMiddleware
on_poll - OnPoolMiddleware

OnUpdateMiddleware - middleware that reacts to everything
```
everything you can import from
```text
from pyrogram_patch.middlewares.middleware_types
```

# FSM
now work just with on_message

## Usage

```python
from pyrogram import Client
from pyrogram_patch import patch
from pyrogram_patch.fsm.storages import MemoryStorage

# create client
app = Client("my_account", api_id='API_ID', api_hash='API_HASH')

# patch client
patch_manager = patch(app)

# include middleware
patch_manager.set_storage(MemoryStorage())

```

## Creating state groups

```python
from pyrogram_patch.fsm import StatesGroup, StateItem

class Parameters(StatesGroup):
    weight = StateItem()
    height = StateItem()
```
## Processing and filtering data

```python
from pyrogram_patch.fsm import StateFilter, State


@app.on_message(filters.private & StateFilter()) # the same as StateFilter("*"), catches all states
async def process_1(client: Client, message, state: State):
    if message.text == 'my_data':
        await client.send_message(message.chat.id, 'enter your weight')
        await state.set_state(Parameters.weight)

        
@app.on_message(filters.private & StateFilter(Parameters.weight))
async def process_2(client: Client, message, state: State):
    await state.set_data({'weight': message.text})
    await client.send_message(message.chat.id, 'enter your height')
    await state.set_state(Parameters.height)

    
@app.on_message(filters.private & StateFilter(Parameters.height))
async def process_3(client: Client, message, state: State):
    state_data = await state.get_data()
    weight = state_data['weight']
    await client.send_message(message.chat.id, 'your height - {} your weight - {}'.format(message.text, weight))
    await state.finish()
```
## Writing your own storages
```python
from pyrogram_patch.fsm import State, BaseStorage


class YourStorage(BaseStorage):

    def __init__(self) -> None:
        ...

    async def checkup(self, key) -> State:
        ...


    async def set_state(self, state: str, key: str) -> None:
        ...

    async def set_data(self, data: dict, key: str) -> None:
        ...

    async def get_data(self, key: str) -> dict:
        ...

    async def finish_state(self, key: str) -> None:
        ...

# don't forget to make a pull request to the patch's github 😉
```

## Using filters with outer_middlewares 
now work just with on_message
```python
async def my_filter(_, __, query) -> bool:
    some_data = await query.middleware_helper.get_data('my_value_name')
    await query.middleware_helper.insert_data('some_data', 'some_data' + some_data)
    return True  # False
digit_filter = filters.create(my_filter)
```

## Routers

```python
from pyrogram_patch.router import Router


my_router = Router()

@my_router.on_message(filters.me)
async def my_commands(client, message, my_value_name):
    print(my_value_name)
```

main.py

```python
patch_manager.include_router(my_router)
```

# Contributing
Pull requests are welcome. For major changes, please open a question first to discuss what you would like to change.

Be sure to update tests as needed.

more details: https://kotttee.xyz/docs/pyrogram_patch/

github: https://github.com/kotttee/pyrogram_patch
## License
[MIT](https://choosealicense.com/licenses/mit/)