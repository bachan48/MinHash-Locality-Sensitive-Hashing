default: test

expected:
	./data/test-01.sh > data/test-01.expected
	./data/test-02.sh > data/test-02.expected
	./data/test-03.sh > data/test-03.expected
	./data/test-04.sh > data/test-04.expected
	./data/test-05.sh > data/test-05.expected
	./data/test-06.sh > data/test-06.expected

test01:
	./data/test-01.sh > /tmp/test-01.output
	diff data/test-01.expected /tmp/test-01.output

test02:
	./data/test-02.sh > /tmp/test-02.output
	diff data/test-02.expected /tmp/test-02.output

test03:
	./data/test-03.sh > /tmp/test-03.output
	diff data/test-03.expected /tmp/test-03.output

test04:
	./data/test-04.sh > /tmp/test-04.output
	diff data/test-04.expected /tmp/test-04.output

test05:
	./data/test-05.sh > /tmp/test-05.output
	diff data/test-05.expected /tmp/test-05.output

test06:
	./data/test-06.sh > /tmp/test-06.output
	diff data/test-06.expected /tmp/test-06.output


test: test01 test02 test03 test04 test05 test06

zip:	dataFiles.txt
	rm -f data.zip
	zip data.zip -@ < dataFiles.txt
	cp -pf data.zip /tmp/

