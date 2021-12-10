import functools
def flatMap(f, l):
    ll = map(f, l)
    return functools.reduce(lambda x, y:x+y, ll, [])
print(flatMap(lambda x: [x+1], [1,2,3]))

