import time
import contextlib
import re
import io
from functools import wraps
from neo4j import GraphDatabase


def time_trace(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        st = time.time()
        rt = func(*args, **kwargs)
        print(f'### 실행시간: {time.time() - st:.3f}sec')
        return rt

    return wrapper

class Neo4jHandler:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def query(self, query, parameters=None, db=None):
        assert self.driver is not None, "Driver not initialized!"
        with self.driver.session(database=db) as session:
            try:
                return list(session.run(query, parameters))
            except Exception as e:
                print("Query failed:", e)
    
    @time_trace
    def query_set_operation(self, operation_name, l1, l2, is_jena=False):
        plugin_name = 'operation'
        if is_jena:
            plugin_name = 'jenaOperation'

        query = f"""
        MATCH (n:{l1})
        MATCH (m:{l2})

        WITH COLLECT(n) AS n_list, COLLECT(m) AS m_list                      
        CALL gspatial.{plugin_name}('{operation_name}', [n_list, m_list]) YIELD result   
                    
        UNWIND result AS res
        WITH res[1] AS n, res[2] AS m, res[0] AS results
        WHERE n <> m

        RETURN n.idx, m.idx, results
        """
        self.query(query)

    @time_trace
    def query_operation(self, operation_name, l1, l2, is_jena=False):
        plugin_name = 'operation'
        if is_jena:
            plugin_name = 'jenaOperation'

        query = f"""
        MATCH (n:{l1})
        MATCH (m:{l2})

        WITH COLLECT(n) AS n_list, COLLECT(m) AS m_list                      
        CALL gspatial.{plugin_name}('{operation_name}', [n_list, m_list]) YIELD result   
                    
        UNWIND result AS res
        WITH res[1] AS n, res[2] AS m, res[0] AS results

        WHERE n <> m AND results = true
        RETURN n.idx, m.idx
        """
        self.query(query)

    @time_trace
    def query_dual_operation(self, operation_name, l1, l2, is_jena=False):
        plugin_name = 'operation'
        if is_jena:
            plugin_name = 'jenaOperation'
        
        query = f"""
        MATCH (n:{l1})
        MATCH (m:{l2})

        WITH COLLECT(n) AS n_list, COLLECT(m) AS m_list                      
        CALL gspatial.{plugin_name}('{operation_name}', [n_list, m_list]) YIELD result   
                    
        UNWIND result AS res
        WITH res[1] AS n, res[2] AS m, res[0] AS results

        WHERE n <> m
        RETURN n.idx, m.idx, results
        """
        self.query(query)

    @time_trace
    def query_param_operation(self, operation_name, l, param, is_jena=False):
        plugin_name = 'operation'
        if is_jena:
            plugin_name = 'jenaOperation'
        
        query = f"""
        MATCH (n:{l})
        CALL gspatial.{plugin_name}('{operation_name}', [[n.geometry], [{param}]]) YIELD result
        
        UNWIND result AS res
        WITH n, res[0] AS result

        RETURN n.idx, result;
        """
        self.query(query)

    @time_trace
    def query_single_operation(self, operation_name, l, is_jena=False):
        plugin_name = 'operation'
        if is_jena:
            plugin_name = 'jenaOperation'
        
        query = f"""
        MATCH (n:{l})
        WITH n, collect(n.geometry) AS geometries
        CALL gspatial.{plugin_name}('{operation_name}', [geometries]) YIELD result

        UNWIND result AS res
        WITH n, res[0] AS result

        RETURN n.idx, result
        """
        self.query(query)

# 입력된 operation_name에 따라 적절한 operation을 실행
def match_operation(handler, operation_name, p1, p2, is_jena):
    if operation_name in ["contains", "covers", "covered_by", "crosses", "disjoint", "equals", "intersects", "overlaps", "touches", "within"]:
        handler.query_operation(operation_name, p1, p2, is_jena)
    if operation_name in ["difference", "intersection", "union", "sym_difference"]:
        handler.query_set_operation(operation_name, p1, p2, is_jena)
    if operation_name in ["distnace"]:
        handler.query_dual_operation(operation_name, p1, p2, is_jena)
    if operation_name in ["buffer"]:
        handler.query_param_operation(operation_name, p1, p2, is_jena)
    if operation_name in ["envelope", "convex_hull", "boundary", "centroid"]:
        handler.query_single_operation(operation_name, p1, is_jena)

# 테스트 전에 미리 operation과 jenaOperation에서 10번씩 연산 실행.
def boiling_test(operation_name, p1, p2):
    with contextlib.redirect_stdout(io.StringIO()): # print 출력 방지
        test_operation_10times(operation_name, p1, p2, is_jena=False)
        test_operation_10times(operation_name, p1, p2, is_jena=True)

# operation을 10번씩 실행
def test_operation_10times(operation_name, p1, p2, is_jena):
    handler = Neo4jHandler("neo4j://localhost:7687", "neo4j", "00000000")
    for i in range(10):
        match_operation(handler, operation_name, p1, p2, is_jena)
    handler.close()

# operation과 jenaOperation에서 테스트 실행하고 결과를 파일에 저장
def test_comparison(operation_name, p1, p2):
    with open(f'test_results/test_{p1}_{operation_name}_{p2}.log', 'w') as f:
        with contextlib.redirect_stdout(f):
            boiling_test(operation_name, p1, p2)
            print(f"Test {operation_name} operation 10 times on operation")
            test_operation_10times(operation_name, p1, p2, is_jena=False)
            print(f"Test {operation_name} operation 10 times on jenaOperation")
            test_operation_10times(operation_name, p1, p2, is_jena=True)
    
    # 테스트 결과 log에 저장
    avg_op_time, avg_jena_op_time = calculate_average_times(operation_name, p1, p2)
    
    # log 내용 불러와 평균 시간 계산
    with open(f'test_results/test_{p1}_{operation_name}_{p2}.log', 'a') as f:
        with contextlib.redirect_stdout(f):
            print(f"Average operation time: {avg_op_time:.3f}sec")
            print(f"Average jenaOperation time: {avg_jena_op_time:.3f}sec")
    print(f"Average operation time: {avg_op_time:.3f}sec")
    print(f"Average jenaOperation time: {avg_jena_op_time:.3f}sec")

# 평균 시간 계산
def calculate_average_times(operation_name, p1, p2):
    log_filename = f'test_results/test_{p1}_{operation_name}_{p2}.log'
    with open(log_filename, 'r') as f:
        log_data = f.read()

    all_operation_times = re.findall(r"### 실행시간: ([\d\.]+)sec", log_data)
    
    operation_times = [float(time) for time in all_operation_times[:10]]  # 처음 10개는 operation
    jena_operation_times = [float(time) for time in all_operation_times[10:]]  # 다음 10개는 jenaOperation

    # 평균 계산
    avg_operation_time = sum(operation_times) / len(operation_times)
    avg_jena_operation_time = sum(jena_operation_times) / len(jena_operation_times)
    
    return avg_operation_time, avg_jena_operation_time

# 테스트 실행
if __name__ == '__main__':
    test_comparison('intersection', 'GoodWayToWalk', 'GoodWayToWalk')
    test_comparison('contains', 'AgendaArea', 'Apartment')
    test_comparison('within', 'Apartment', 'AgendaArea')
    test_comparison('intersects', 'AgendaArea', 'AgendaArea')
    test_comparison('boundary', 'AgendaArea', None)
    test_comparison('convex_hull', 'AgendaArea', None)
    test_comparison('envelope', 'AgendaArea', None)