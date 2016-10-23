s = ""
for i in range (0, 278):
	num = str(i)
	while len(num) != 5:
		num = "0" + num
	s += "wget https://s3.amazonaws.com/p42task2whdawn/part-" + num

	if i != 277:
		s += " && "

print(s)