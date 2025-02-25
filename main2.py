
from _connect import _connect as _connect_

def main():
    aws_object = _connect_.get_object("awss3")
    from pprint import pprint
    pprint(aws_object.list_buckets())




if __name__ == '__main__':
    main()