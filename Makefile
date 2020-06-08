# src/bin/pg_waldump/Makefile

PGFILEDESC = "lw_waldump - decode and display WAL"
PGAPPICON=win32

top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

OBJS = \
	$(WIN32RES) \
	lw_waldump.o \
	xlogreader.o

override CPPFLAGS := -DFRONTEND $(CPPFLAGS)


all: lw_waldump

lw_waldump: $(OBJS) | submake-libpgport
	$(CC) $(CFLAGS) $^ $(LDFLAGS) $(LDFLAGS_EX) $(LIBS) -o $@$(X)

xlogreader.c: % : $(top_srcdir)/src/backend/access/transam/%
	rm -f $@ && $(LN_S) $< .

install: all installdirs
	$(INSTALL_PROGRAM) lw_waldump$(X) '$(DESTDIR)$(bindir)/lw_waldump$(X)'

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(bindir)'

uninstall:
	rm -f '$(DESTDIR)$(bindir)/lw_waldump$(X)'

clean distclean maintainer-clean:
	rm -f pg_waldump$(X) $(OBJS) xlogreader.c
	rm -rf tmp_check

check:
	$(prove_check)

installcheck:
	$(prove_installcheck)
