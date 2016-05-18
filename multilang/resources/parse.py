import storm


class ParserBolt(storm.BasicBolt):
    def process(self, tuple):
        # open('/home/roy/output.txt', 'a').write(str(tuple) + '\n')
        id = tuple.values[0]
        result = tuple.values[1] + ' -- parsing by Python'
        storm.emit([id, result])


ParserBolt().run()
