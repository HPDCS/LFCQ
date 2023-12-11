f1 = open("tmp.numa.conf")
f2 = open("tmp.cpu.list")

count = 0
res = ""
for line in f1.readlines():
	count+=1
	res += line.strip().split(":")[1].strip()+","
res = res[:-1]

res2 = res.split(",")
res = ""
for i in res2:
        li = i.split("-")
        if len(li) > 1:
                a = int(li[0])
                b = int(li[1])
                while a<=b:
                        res+=str(a)+","
                        a+=1
        else:
                res+=i+","
resA = res[:-1]



f1.close()

count = 0
res = ""
for line in f2.readlines():
        count+=1
        res += line.strip().split(":")[1].strip()+","
res = res[:-1]

res2 = res.split(",")
res = ""
for i in res2:
	li = i.split("-")
	if len(li) > 1:
		a = int(li[0])
		b = int(li[1])
		while a<=b:
			res+=str(a)+","
			a+=1
	else:	
		res+=i+","
resB = res[:-1]

if resA:
	print resA
else:
	print resB

