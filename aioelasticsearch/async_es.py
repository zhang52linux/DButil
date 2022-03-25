# _*_ coding:utf-8 _*_
import time
import json
import asyncio
import traceback
from loguru import logger
from elasticsearch import AsyncElasticsearch
from elasticsearch.exceptions import ConnectionError, ConnectionTimeout
# 基于scroll id 做的查询，适合深度查询，查询会创建scroll context ,有状态, 默认500个，超过后会报错, 深度查询推荐search after


class ESGetter:

    def __init__(self, es, index, doc_type=None, body=None, scroll="3m", retry=5):
        self.es = es
        self.index = index
        self.doc_type = doc_type
        self.body = body
        self.scroll = scroll
        self.retry = retry
        self.sc_id = None
        self.total_size = None
        self.fetch_count = 0

    async def get_data(self):
        if self.total_size is None:
            result = await self.es.client.search(index=self.index, doc_type=self.doc_type, body=self.body, scroll=self.scroll)
            self.total_size = result['hits']['total']['value']
            if self.total_size == 0:
                await self.es.client.close()
                raise StopAsyncIteration
        elif self.fetch_count >= self.total_size or self.sc_id is None:
            await self.es.client.close()
            raise StopAsyncIteration
        else:
            result = await self.es.client.scroll(scroll_id=self.sc_id, scroll=self.scroll)
        if not len(result['hits']['hits']):
            logger.info("the fetch count {} is inconsistent with the total count {}".format(self.fetch_count, self.total_size))
            await self.es.client.close()
            raise StopAsyncIteration
        self.fetch_count += len(result['hits']['hits'])
        self.sc_id = result.get("_scroll_id")
        return result['hits']['hits']

    def __aiter__(self):
        return self

    async def __anext__(self, retry=1):
        try:
            st = time.time()
            result = await self.get_data()
            finished_rate = self.fetch_count / self.total_size if self.total_size else 1
            logger.info("fetch count {} from {}, total count {}, finished {:.5f}%, use time: {}".format(self.fetch_count, self.index, self.total_size, finished_rate * 100, round(time.time() - st, 3)))
            return result
        except StopAsyncIteration:
            await self.es.client.close()
            raise
        except Exception as e:
            if isinstance(e, (ConnectionTimeout, ConnectionError)):
                self.es.reconnect()
            if self.retry > retry:
                await asyncio.sleep(1)
                return await self.__anext__(retry + 1)
            else:
                raise


class ESWriter:

    def __init__(self, es, index, doc_type=None, action="index", timeout=30, retry=5):
        """
        action: index/create/update/delete
        """
        self.es = es
        self.index = index
        self.doc_type = doc_type
        self.action = action
        self.retry = retry
        self.timeout = timeout
        self.total_count = 0
        self.succ_count = 0
        self.fail_count = 0

    async def write(self, documents=None, timeout=None, raise_error=True):
        if not documents:
            return (0, [])
        body = self.es.gen_bulk_body(documents, self.index, self.doc_type, self.action)
        self.total_count += len(documents)
        for i in range(self.retry):
            try:
                st = time.time()
                resp = await self.es.async_bulk(body, timeout or self.timeout, raise_error)
                self.succ_count += resp["success"]
                self.fail_count += resp["fail"]
                logger.info("write success count {}, total saved {}, successed {}, use time: {}".format(resp["success"], self.total_count, self.succ_count, round(time.time() - st, 3)))
                return resp
            except Exception:
                if not await self.es.client.ping():
                    self.es.reconnect()
                if i + 1 == self.retry and raise_error:
                    raise
                else:
                    self.fail_count += len(documents)


class Elasticsearch:

    def __init__(self, hosts, headers=None):
        self.hosts = hosts
        self.headers = headers
        self.client = AsyncElasticsearch(hosts=hosts, timeout=30, headers=headers)

    def reconnect(self):
        self.client = AsyncElasticsearch(hosts=self.hosts, timeout=30, headers=self.headers)

    def getter(self, index, doc_type=None, body=None, scroll="3m", retry=5):
        return ESGetter(self, index, doc_type=doc_type, body=body, scroll=scroll, retry=retry)

    def writer(self, index, doc_type=None, action="index", retry=5):
        return ESWriter(self, index, doc_type=doc_type, action=action, retry=retry)

    async def exists(self, ids, index, doc_type=None):
        resp = await asyncio.gather(*[self.client.exists(index, _id, doc_type=doc_type) for _id in ids])
        return resp

    def gen_bulk_body(self, documents, index=None, doc_type=None, action="index"):
        body = ""
        for doc in documents:
            _op_type = doc.pop("_action", action).lower()
            action_ = {_op_type: {
                "_id": doc.pop("_id"),
                "_index": doc.pop("_index", index),
                "_type": doc.pop("_type", doc_type)
            }}
            if _op_type == "update":
                doc = {"doc": doc}
            body += json.dumps(action_) + "\n" + json.dumps(doc) + "\n"
        return body

    async def async_bulk(self, body, timeout=None, raise_error=True):
        try:
            success = fail = 0
            resp = await self.client.transport.perform_request("POST", "/_bulk?pretty", params={"request_timeout": timeout}, body=body, headers=self.headers)
            if resp["errors"]:
                for item in resp["items"]:
                    for k, v in item.items():
                        if "error" in v:
                            if raise_error:
                                logger.error(json.dumps(v["error"]))
                            fail += 1
                        else:
                            success += 1
            else:
                success = len(resp["items"])
            resp["success"] = success
            resp["fail"] = fail
            return resp
        except Exception:
            if raise_error:
                await self.client.close()
                raise
            return {"sucess": 0, "fail": 0, "errors": str(traceback.format_exc())}

    async def fetch(self, index, doc_type=None, body=None, retry=5, raise_error=True):
        try:
            return await self.client.search(index=index, doc_type=doc_type, body=body)
        except Exception:
            if retry:
                return await self.fetch(index, doc_type, body, retry - 1, raise_error)
            elif raise_error:
                await self.client.close()
                raise
            else:
                logger.error(traceback.format_exc())
