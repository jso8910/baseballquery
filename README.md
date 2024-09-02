# Baseball Query

It's a stathead replacement, plain and simple.

When you install this package and import it for the first time, it will download many GB of data from retrosheet. Eventually, it will be deleted, but you will get a total of 12 GB of data in the form of an hdf5 file. This is a lot of data, but it's necessary. This whole process (including calculating linear weights) can take upwards of an hour so start running this in the background once you install it before you use it.

Not implemented (as of when I finish this):
- Park factors
- Full game stats (saves, holds, shutouts, etc.) for pitchers. This one is probably important
- GB%, LD%, FB%, and PU% will deviate from fangraphs due to differences in data and it being quite subjective. Also, Fangraphs FB is more similar to FB+PU so that's what I used in HR/FB% calculations.
    - This probably is impossible to fix


Recipients of Retrosheet data are free to make any desired use of
the information, including (but not limited to) selling it,
giving it away, or producing a commercial product based upon the
data.  Retrosheet has one requirement for any such transfer of
data or product development, which is that the following
statement must appear prominently:

     The information used here was obtained free of
     charge from and is copyrighted by Retrosheet.  Interested
     parties may contact Retrosheet at "www.retrosheet.org".

Retrosheet makes no guarantees of accuracy for the information 
that is supplied. Much effort is expended to make our website 
as correct as possible, but Retrosheet shall not be held 
responsible for any consequences arising from the use of the 
material presented here. All information is subject to corrections 
as additional data are received. We are grateful to anyone who
discovers discrepancies and we appreciate learning of the details.
