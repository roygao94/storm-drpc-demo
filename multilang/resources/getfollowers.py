import storm

FOLLOWERS_DB = {
    "sally": ["bob", "tim", "alice", "adam", "jim", "chris", "jai"],
    "bob": ["sally", "nathan", "jim", "mary", "david", "vivian"],
    "tim": ["alex"],
    "nathan": ["sally", "bob", "adam", "harry", "chris", "vivian", "emily", "jordan"],
    "adam": ["david", "carissa"],
    "mike": ["john", "bob"],
    "john": ["alice", "nathan", "jim", "mike", "bob"]
}


class GetFollowersBolt(storm.BasicBolt):
    def process(self, tuple):
        id = tuple.values[0]
        tweeter = tuple.values[1]
        if tweeter in FOLLOWERS_DB:
            followers = FOLLOWERS_DB[tweeter]
            for follower in followers:
                storm.emit([id, follower])


GetFollowersBolt().run()
