f = open("hard.txt")

res = ""
d = {}
for line in f.readlines():
	if "processor" in line:
		res = "P: "
		p = line.strip().split(":")[1].strip()
	if "physical id" in line:
		n = line.strip().split(":")[1].strip()
	if "core id" in line:
		c = line.strip().split(":")[1].strip()
	if "processor" in line or "core id" in line or "physical id" in line:
		res +=  line.strip().split(":")[1].strip()+"\t"
	if "core id" in line:
		n = int(n)
		p = int(p)
		c = int(c)
		if n not in d:
			d[n] = {}
		if c not in d[n]:
			d[n][c] = []
		d[n][c] += [p]
		print res
		res = ""


for n in sorted(d):
	print "NUMA " +str(n)
	for c in sorted(d[n]):
		print "|-- CORE "+str(c)
		print "     |-- "+str(d[n][c])


res = ""
for n in sorted(d):
        #nt "NUMA " +str(n)
        for c in sorted(d[n]):
                #print "|-- CORE "+str(c)
		for f in d[n][c]:
	                res+=str(f)+","
print res[:-1]
