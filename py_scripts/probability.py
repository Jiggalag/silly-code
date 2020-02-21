cash = 100

for step in range(100):
    if step % 2 == 0:
        cash = cash * 1.5
        print('iteration {}, value = {}'.format(step, cash))
    else:
        cash = cash * 0.6
        print('iteration {}, value = {}'.format(step, cash))

print(cash)