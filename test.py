    
class C :
    x = 10
    y = 99
    z = "string"



s = "abc\tFXX\t123\tG11"

L = (s.rsplit("\t"))
print(L)
L.sort()
print(L)
L.remove(L[0])
print(L)
