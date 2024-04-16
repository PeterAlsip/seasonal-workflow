from calendar import isleap, monthrange
from textwrap import dedent


def write_xml(common, ystart, mstart):
    ystart = int(ystart)
    mstart = int(mstart)

    if isleap(ystart) and mstart == 3:
        mfirst = 2
        dfirst = 29
    else:
        mfirst = mstart
        dfirst = 1

    if mstart == 1:
        yend = ystart
        mlast = 12
    else:
        yend = ystart + 1
        mlast = mstart - 1

    dlast = monthrange(yend, mlast)[1]

    xml = dedent(f'''    <?xml version="1.0"?>
    <!DOCTYPE doc [
      <!ENTITY common SYSTEM "{common}">
    ]>

    <experimentSuite rtsVersion="4" xmlns:xi="http://www.w3.org/2001/XInclude">

      <property name="ystart" value="{ystart}"/>
      <property name="mstart" value="{mstart:02d}"/>
      <property name="atmosspan" value="{ystart}{mfirst:02d}{dfirst:02d}-{yend}{mlast:02d}{dlast:02d}"/>

      &common;
      
    </experimentSuite>''')

    fname = common.replace('_common.xml', '')
    with open(f'{fname}_{ystart}-{mstart:02d}.xml', 'w') as f:
        f.write(xml)


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('common', type=str)
    parser.add_argument('ystart', type=int)
    parser.add_argument('mstart', type=int)
    args = parser.parse_args()
    write_xml(args.common, args.ystart, args.mstart)

