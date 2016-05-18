import storm


class ParserBolt(storm.BasicBolt):
    def process(self, tup):
        open('/home/roy/output.txt', 'a').write(str(tup) + '\n')
        # id = tup.values[0]
        # result = tup.values[1] + ' -- parsing by Python'
        storm.emit([tup.values[0] + ' -- parsing by Python', tup.values[1]])


ParserBolt().run()
