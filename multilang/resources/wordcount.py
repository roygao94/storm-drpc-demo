import storm


class WordCount(storm.BasicBolt):
	def __init__(self):
		self.counts = {}

	def process(self, tuple):
		word = tuple.values[0]
		if word not in self.counts:
			count = 0
		else:
			count = self.counts[word]
		count += 1
		self.counts[word] = count
		storm.emit([word, count])


WordCount().run()
