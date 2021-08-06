reset

set terminal pdf
set output 'workloade.pdf';

set style data histogram
set style histogram errorbars gap 2 lw 1

set title "ycsb worload-e"

set ylabel "time(us)/req"
set auto x

set key outside below center

# set log y
set style fill pattern border
set grid
plot "data_workloade.dat" using 2:3:xtic(1) with histogram title column(2), \
        '' using 4:5:xtic(1) with histogram title column(4)
set output