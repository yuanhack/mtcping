#
# $Id: Makefile,v 1.18 2004/11/01 16:22:03 mkirchner Exp $
#

FILES=*.c *.h *.cpp *.a Makefile *.conf *.list 
VERNUM=`grep VERSION mtcping.c | cut -d" " -f3`
VER=mtcping-$(VERNUM)

CC=g++

CCFLAGS  = -Wall
CCFLAGS += -g
CCFLAGS += -D_REENTRANT
#CCFLAGS += -I./include/mysql
CLIBS_CENTOS64_LNK = -Wl,-rpath,/usr/lib64/mysql -L/usr/lib64/mysql 
CLIBS += -lmysqlclient -lpthread -lrt

all: tcping mtcping mtraceroute

mtcping: mtcping.c yhlog.c read_conf.c ping.cpp yhnet.c
	$(CC) -o $@ $(CCFLAGS) $^ ${CLIBS}

tcping: tcping.c
	$(CC) -o tcping $(CCFLAGS) -DHAVE_HSTRERROR tcping.c 

mtraceroute: mtraceroute.c yhlog.c read_conf.c yhnet.c
	$(CC) -o $@ $(CCFLAGS) $^ $(CLIBS) 

s: mtcping tcping mtraceroute
	sudo chown root mtcping mtraceroute && sudo chmod +s mtcping mtraceroute
us: mtcping tcping mtraceroute
	sudo chown root mtcping mtraceroute && sudo chmod -s mtcping mtraceroute
fedora64: centos64
centos64: mtcping64 mtraceroute64 tcping64
mtcping64: mtcping.c yhlog.c read_conf.c ping.cpp yhnet.c
	$(CC) -o mtcping $(CCFLAGS) $^ $(CLIBS_CENTOS64_LNK) ${CLIBS}
mtraceroute64: mtraceroute.c yhlog.c read_conf.c yhnet.c
	$(CC) -o mtraceroute $(CCFLAGS) $^ $(CLIBS_CENTOS64_LNK)  $(CLIBS) 
tcping64: tcping.c
	$(CC) -o tcping $(CCFLAGS) -DHAVE_HSTRERROR tcping.c 

clean:
	rm -rf tcping core *.o mtcping *.tar.gz mtraceroute $(VER)

exe: mtcping tcping mtraceroute
	mkdir $(VER) ; cp -rf mtcping mtraceroute *.conf $(VER)/
exe64: centos64
	mkdir $(VER) ; cp -rf mtcping mtraceroute *.conf $(VER)/

export: mtcping tcping mtraceroute
	mkdir $(VER) ; cp -rf mtcping mtraceroute *.conf $(VER)/; tar -c $(VER) | gzip -9 > $(VER).tar.gz ; rm -rf $(VER)
export64: centos64
	mkdir $(VER) ; cp -rf mtcping mtraceroute *.conf $(VER)/; tar -c $(VER) | gzip -9 > $(VER).tar.gz ; rm -rf $(VER)

dist:
	mkdir $(VER) ; cp -rf $(FILES) $(VER)/ ; tar -c $(VER) | gzip -9 > $(VER).tar.gz ; rm -rf $(VER)
