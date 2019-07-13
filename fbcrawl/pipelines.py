# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html

import scrapy
from scrapy.exceptions import DropItem
from postgres.models import Post, Group
from postgres.settings import postgres_database
from datetime import datetime
from scrapy.pipelines.images import ImagesPipeline
from twisted.internet.defer import Deferred, DeferredList, _DefGen_Return
from twisted.python.failure import Failure
from scrapy.utils.misc import arg_to_iter
import shortuuid

class FbcrawlPipeline(object):
    pass
#    def process_item(self, item, spider):
#        if item['date'] < datetime(2017,1,1).date():
#            raise DropItem("Dropping element because it's older than 01/01/2017")
#        elif item['date'] > datetime(2018,3,4).date():
#            raise DropItem("Dropping element because it's newer than 04/03/2018")
#        else:
#            return item

class PostImagePipeline(ImagesPipeline):

    def process_item(self, item, spider):
        # ensure 
        if abs(spider.count) + 1 > spider.max:
            spider.close_down = True
        info = self.spiderinfo
        requests = arg_to_iter(self.get_media_requests(item, info))
        dlist = [self._process_request(r, info) for r in requests]
        dfd = DeferredList(dlist, consumeErrors=1)
        # only update when item is passed to pipeline, ensuring count consistency
        spider.count -= 1
        return dfd.addCallback(self.item_completed, item, info)

    def get_media_requests(self, item, info):
        if 'image_urls' in item:
            for img_url in item['image_urls']:
                yield scrapy.Request(img_url)
    
    def item_completed(self, results, item, info):
        item['image_paths'] = []
        if len(results) > 0:
            image_paths = [x['path'] for ok, x in results if ok]
            if not image_paths:
                raise DropItem("Item contains no images")
            item['image_paths'] = image_paths
        success = True
        if 'source' not in item:
            raise DropItem("Dropping element because doesnt have a source name")
        if len(item['source']) < 2:
            group_id = item['url'].split('/')[2].split('?')[0]
            query = Group.select().where(Group.uuid == group_id)
            if query.exists():
                item['source'].append(query[0].name)
            else:
                raise DropItem("Dropping element because doesnt have a source name")
        if 'date' in item and len(item['date']) == 0:
            raise DropItem("No date found")
        if 'text' not in item:
            raise DropItem("Do not have or do not contain any valid text")

        if len(item['text']) < 10:
            raise DropItem("Incomplete info")
        try:
            if Post.select().where(Post.post_id==item['post_id']).exists():
                with postgres_database.atomic():
                    Post.update(
                        images=item['image_paths'],
                        date=item['date'][0],
                        reactions = item['reactions'] if 'reactions' in item and item['reactions'] else 0,
                        likes = item['likes'] if 'likes' in item and item['likes'] else 0
                        ).where(Post.post_id == item['post_id']).execute()
            else:
                with postgres_database.atomic():

                    if len(item['source'][1]) > 0 and len(Group.select().where(Group.name == item['source'][1]) ) == 0:
                        Group.create(
                            uuid=item['url'].split('/')[2].split('?')[0],
                            name=item['source'][1]
                        )
                    else:
                        Group.update(
                            uuid=item['url'].split('/')[2].split('?')[0]
                        ).where(Group.name == item['source'][1]).execute()


                    post = Post.create(
                            post_id=item['post_id'],
                            source=item['source'][0],
                            text=item['text'],
                            shared_in=item['source'][1],
                            url=item['url'],
                            images=item['image_paths'],
                            date=item['date'][0],
                            reactions = item['reactions'] if 'reactions' in item and item['reactions'] else 0,
                            likes = item['likes'] if 'likes' in item and item['likes'] else 0,
                            ahah = item['ahah'] if 'ahah' in item and item['ahah'] else 0,
                            love = item['love'] if 'love' in item and item['love'] else 0,
                            wow = item['wow'] if 'wow' in item and item['wow'] else 0,
                            sigh = item['sigh'] if 'sigh' in item and item['sigh'] else 0,
                            grr = item['grrr'] if 'grrr' in item and item['grrr'] else 0,
                            comment_count = item['comments'] if 'comments' in item and item['comments'] else 0)
        except BaseException as e:
            raise DropItem("Exception : {}".format(str(e)))
        return item

class PostgresPostPipeline(object):
    '''Original pipeline
    '''

    def process_item(self, item, spider):
        success = True
        if len(item['source']) < 2:
            raise DropItem("Dropping element because doesnt have a source name")
        if 'date' in item and len(item['date']) == 0:
            raise DropItem("No date found")
        if 'text' not in item:
            raise DropItem("Do not have or do not contain any valid text")
        if len(item['text']) < 10:
            raise DropItem("Incomplete info")
        try:
            query = Post.select().where(Post.post_id==item['post_id'])
            if not query.exists():
                with postgres_database.atomic():
                    if len(item['source'][1]) > 0 and len(Group.select().where(Group.name == item['source'][1]) ) == 0:
                        Group.create(
                            uuid=str(shortuuid.uuid())[:20],
                            name=item['source'][1]
                        )
                    post = Post.create(
                            post_id=item['post_id'],
                            source=item['source'][0],
                            text=item['text'],
                            shared_in=item['source'][1],
                            url=item['url'],
                            date=item['date'][0],
                            reactions = item['reactions'] if 'reactions' in item and item['reactions'] else 0,
                            likes = item['likes'] if 'likes' in item and item['likes'] else 0,
                            ahah = item['ahah'] if 'ahah' in item and item['ahah'] else 0,
                            love = item['love'] if 'love' in item and item['love'] else 0,
                            wow = item['wow'] if 'wow' in item and item['wow'] else 0,
                            sigh = item['sigh'] if 'sigh' in item and item['sigh'] else 0,
                            grr = item['grrr'] if 'grrr' in item and item['grrr'] else 0,
                            comment_count = item['comments'] if 'comments' in item and item['comments'] else 0)
        except BaseException as e:
            raise DropItem("Exception : {}".format(str(e)))
        return item

