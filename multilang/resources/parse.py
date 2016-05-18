import storm


class ParserBolt(storm.BasicBolt):
    def process(self, tuple):
        id = tuple.values[0]
        result = tuple.values[1] + "##parser"
        storm.emit([id, result])


ParserBolt().run()
