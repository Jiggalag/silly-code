yearly_save = 900
percent = 0.06
total = 1000
y = 2020
for year in range(25):
    total = total + yearly_save + total*percent
    print('in {} year total equals {}'.format(y + year, total))
