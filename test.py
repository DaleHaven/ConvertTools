
import ujson

if __name__ == '__main__':
    testStr = """
    {"name":"DaleHaven","age":12}
    """
    print(','.join(ujson.loads(testStr).values()))