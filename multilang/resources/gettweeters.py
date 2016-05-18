import storm

TWEETERS_DB = {
    "foo.com/blog/1": ["sally", "bob", "tim", "george", "nathan"],
    "engineering.twitter.com/blog/5": ["adam", "david", "sally", "nathan"],
    "tech.backtype.com/blog/123": ["tim", "mike", "john"]
}


class GetTweetersBolt(storm.BasicBolt):
    def process(self, tuple):
        id = tuple.values[0]
        url = tuple.values[1]
        if url in TWEETERS_DB:
            tweeters = TWEETERS_DB[url]
            for tweeter in tweeters:
                storm.emit([id, tweeter])

GetTweetersBolt().run()
