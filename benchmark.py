import timeit

# No transfers      : 19051 -> 18111
# Single transfer   : 19051 -> 18129
# Equally optimal   : 59039 -> 54589
# Loops             : 11389 -> 11381
# LS                : 19051 -> 03381
# Tim               : 18129 -> 10199
# Skipped stops     : 59119 -> 63091

def main():
    number = 10
    print(timeit.timeit("dijkstra('59039', '54589')", setup="from route import dijkstra", number=number) / number)

if __name__ == '__main__':
    main()
