from pathlib import Path
import bcrypt
from pymongo import MongoClient
from pymongo.server_api import ServerApi
import urllib
import urllib.parse
from dotenv import load_dotenv,find_dotenv
import streamlit as st
import streamlit_authenticator as stauth

def hash_passwords(password):
  salt = bcrypt.gensalt()
  hashed = bcrypt.hashpw(password.encode('utf-8'), salt)

  return hashed.decode('utf-8')


if __name__ == "__main__":

  name = "admin"
  username = "admin"
  passwords = "admin123"

  #hashed_passwords = [hash_passwords(password) for password in passwords]
  hashed_passwords = stauth.Hasher([passwords]).generate()

  mongo_user = "camppoAdmin"
  mongo_pass = "C4mppoP@ss"

  username_mongo = urllib.parse.quote_plus(mongo_user)
  password_mongo = urllib.parse.quote_plus(mongo_pass)

  client = MongoClient("mongodb+srv://%s:%s@cluster0.gjkin5a.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0" % (username_mongo, password_mongo))
  db = client.personalAI
  coll = db.usuarios

  new_user = {
    "name": name,
    "username": username,
    "password":hashed_passwords}

  result = coll.insert_one(new_user)

  print(f"Documentos inseridos com os IDs: {result.inserted_id}")