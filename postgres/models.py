from peewee import *
from postgres.settings import postgres_database, TIMEZONE
from playhouse.postgres_ext import ArrayField
from datetime import datetime
from pytz import timezone
import shortuuid

def timezone_now():
    return datetime.now().replace(tzinfo=timezone(TIMEZONE))

class Post(Model):
    post_id = CharField(unique=True, index=True)
    source = CharField()
    date = DateField(index=True)
    text = TextField()
    url = CharField(unique=True)
    shared_in = CharField()

    reactions = IntegerField(default=0)
    likes = IntegerField(default=0)
    ahah = IntegerField(default=0)
    love = IntegerField(default=0)
    wow = IntegerField(default=0)
    sigh = IntegerField(default=0)
    grr = IntegerField(default=0)
    comment_count = IntegerField(default=0)
    images = ArrayField(CharField, default=[])
    inserted = DateTimeField(default=timezone_now)

    class Meta:
        database = postgres_database

class Group(Model):
    uuid = CharField(unique=True, index=True)
    name = CharField(unique=True, index=True)

    class Meta:
        database = postgres_database
