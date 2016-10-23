s = ""
for i in range (1, 278):
	num = str(i)
	while len(num) != 5:
		num = "0" + num
	s += "aws s3 cp /mnt/Project4_2/spark-1.5.2-bin-hadoop2.6/bin/task2/part-" + num + " s3://p42task2whdawn"

	if i != 277:
		s += " && "

print(s)