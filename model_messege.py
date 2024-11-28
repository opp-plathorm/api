from pydantic import BaseModel

class Message(BaseModel):
    key: str
    value: str

class Login(BaseModel):
    login:str
    password:str