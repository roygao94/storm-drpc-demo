import storm


class ExclaimBolt(storm.BasicBolt):
    def process(self, tup):
        input = tup.values[1]
        storm.emit([tup.values[0], input + "!!!python!!!"])

ExclaimBolt().run()
