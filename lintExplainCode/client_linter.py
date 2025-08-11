#!/usr/bin/env python3
"""
Client MongoDB Performance Linter
Analyzes PR diffs against client's MongoDB cluster without creating sample data
"""

import os
import re
import sys
import argparse
from typing import List, Dict, Any
from pymongo import MongoClient
from config import config


class ClientLinter:
    """Client-focused MongoDB performance linter"""
    
    def __init__(self):
        self.client = None
        self.db = None
        self._connect()
        self._discover_collections()
    
    def _connect(self):
        """Connect to client's MongoDB cluster"""
        try:
            connection_string = config.mongo.connection_string
            database_name = config.mongo.database
            
            # Handle authentication if credentials provided
            if config.mongo.username and config.mongo.password:
                # If connection string doesn't already have auth, add it
                if '@' not in connection_string:
                    # Parse the connection string to add auth
                    if connection_string.startswith('mongodb://'):
                        # Extract host and port
                        host_part = connection_string.replace('mongodb://', '')
                        connection_string = f"mongodb://{config.mongo.username}:{config.mongo.password}@{host_part}"
                    elif connection_string.startswith('mongodb+srv://'):
                        # For Atlas connections, extract the cluster part
                        cluster_part = connection_string.replace('mongodb+srv://', '')
                        connection_string = f"mongodb+srv://{config.mongo.username}:{config.mongo.password}@{cluster_part}"
                    else:
                        connection_string = f"mongodb://{config.mongo.username}:{config.mongo.password}@localhost:27017"
                
                # Add auth source if specified and not already in connection string
                if config.mongo.auth_source and 'authSource=' not in connection_string:
                    separator = '&' if '?' in connection_string else '?'
                    connection_string += f"{separator}authSource={config.mongo.auth_source}"
            
            print(f"🔗 Connecting to client MongoDB: {database_name}")
            print(f"📡 Connection string: {connection_string.replace(config.mongo.password, '***') if config.mongo.password else connection_string}")
            
            # Connect to client's cluster
            self.client = MongoClient(connection_string)
            self.db = self.client[database_name]
            
            # Test connection
            self.client.admin.command('ping')
            print("✅ Connected to client MongoDB successfully!")
            
        except Exception as e:
            print(f"❌ Failed to connect to client MongoDB: {e}")
            print(f"Connection string used: {config.mongo.connection_string}")
            print(f"Username: {config.mongo.username}")
            print(f"Database: {config.mongo.database}")
            print(f"Auth source: {config.mongo.auth_source}")
            print("\n💡 Make sure:")
            print("   - Client has granted proper permissions")
            print("   - Network access is configured")
            print("   - Connection string is correct")
            raise
    
    def _discover_collections(self):
        """Discover existing collections in client's database"""
        try:
            collections = self.db.list_collection_names()
            
            # Filter out system collections
            user_collections = [c for c in collections if not c.startswith('system.')]
            
            if not user_collections:
                print("⚠️  No user collections found in client database")
                print("💡 Make sure the client has data and proper permissions")
                return
            
            print(f"📚 Discovered {len(user_collections)} collections in client database:")
            
            for collection_name in user_collections:
                try:
                    collection = self.db[collection_name]
                    indexes = list(collection.list_indexes())
                    index_names = [idx['name'] for idx in indexes if idx['name'] != '_id_']
                    
                    # Count documents (be careful with large collections)
                    try:
                        doc_count = collection.count_documents({})
                        print(f"  📊 {collection_name}: {len(index_names)} indexes, {doc_count} documents")
                    except Exception:
                        print(f"  📊 {collection_name}: {len(index_names)} indexes, document count unavailable")
                        
                except Exception as e:
                    print(f"  ⚠️  {collection_name}: Unable to analyze ({e})")
            
        except Exception as e:
            print(f"❌ Failed to discover collections: {e}")
            print("💡 Check if user has 'listCollections' permission")
    
    def extract_queries_from_diff(self, diff_content: str) -> List[Dict[str, Any]]:
        """Extract MongoDB queries from PR diff"""
        queries = []
        
        # Simple regex patterns for common MongoDB operations
        patterns = [
            r'\.find\(([^)]+)\)',           # find operations
            r'\.findOne\(([^)]+)\)',        # findOne operations
            r'\.aggregate\(([^)]+)\)',      # aggregate operations
            r'\.updateOne\(([^)]+)\)',      # updateOne operations
            r'\.updateMany\(([^)]+)\)',     # updateMany operations
            r'\.deleteOne\(([^)]+)\)',      # deleteOne operations
            r'\.deleteMany\(([^)]+)\)',     # deleteMany operations
        ]
        
        lines = diff_content.split('\n')
        for i, line in enumerate(lines):
            for pattern in patterns:
                matches = re.finditer(pattern, line)
                for match in matches:
                    # Try to extract collection name from context
                    collection_name = self._extract_collection_name(line, i, lines)
                    
                    queries.append({
                        'line_number': i + 1,
                        'operation': pattern.split('.')[1].split('(')[0],
                        'query': match.group(1),
                        'collection': collection_name,
                        'full_line': line.strip()
                    })
        
        return queries
    
    def _extract_collection_name(self, line: str, line_num: int, all_lines: List[str]) -> str:
        """Extract collection name from context"""
        # Look for db.collection or collection patterns
        collection_pattern = r'(?:db\.)?(\w+)\.(?:find|findOne|aggregate|update|delete)'
        match = re.search(collection_pattern, line)
        if match:
            return match.group(1)
        
        # Look in previous lines for variable assignments
        for i in range(max(0, line_num - 3), line_num):
            var_pattern = r'(\w+)\s*=\s*db\.(\w+)'
            match = re.search(var_pattern, all_lines[i])
            if match:
                return match.group(2)
        
        return "unknown"
    
    def analyze_query_performance(self, collection_name: str, query_str: str, operation: str) -> Dict[str, Any]:
        """Analyze query performance using explain with executionStats"""
        try:
            # Check if collection exists
            if collection_name not in self.db.list_collection_names():
                return {
                    'collection': collection_name,
                    'operation': operation,
                    'query': query_str,
                    'error': f"Collection '{collection_name}' not found in client database",
                    'execution_time_ms': 0,
                    'documents_examined': 0,
                    'documents_returned': 0,
                    'index_used': None,
                    'is_collection_scan': False
                }
            
            collection = self.db[collection_name]
            
            # Parse the query string (simplified parsing)
            query = self._parse_query_string(query_str)
            
            # Run explain with executionStats
            if operation == 'aggregate':
                # For aggregation, we need to parse the pipeline
                pipeline = self._parse_aggregation_pipeline(query_str)
                explain_result = collection.aggregate(pipeline).explain('executionStats')
            else:
                # For find operations
                explain_result = collection.find(query).explain('executionStats')
            
            # Extract key performance metrics
            execution_stats = explain_result.get('executionStats', {})
            query_planner = explain_result.get('queryPlanner', {})
            
            analysis = {
                'collection': collection_name,
                'operation': operation,
                'query': query_str,
                'execution_time_ms': execution_stats.get('executionTimeMillis', 0),
                'documents_examined': execution_stats.get('totalDocsExamined', 0),
                'documents_returned': execution_stats.get('nReturned', 0),
                'index_used': query_planner.get('winningPlan', {}).get('indexName'),
                'is_collection_scan': query_planner.get('winningPlan', {}).get('stage') == 'COLLSCAN',
                'raw_explain': explain_result
            }
            
            return analysis
            
        except Exception as e:
            return {
                'collection': collection_name,
                'operation': operation,
                'query': query_str,
                'error': str(e),
                'execution_time_ms': 0,
                'documents_examined': 0,
                'documents_returned': 0,
                'index_used': None,
                'is_collection_scan': False
            }
    
    def _parse_query_string(self, query_str: str) -> Dict[str, Any]:
        """Simple query string parser"""
        try:
            # Remove quotes and clean up
            cleaned = query_str.strip().strip("'\"")
            
            # Simple pattern matching for common query structures
            if '{' in cleaned and '}' in cleaned:
                # Try to evaluate as Python dict (be careful with this in production!)
                try:
                    return eval(cleaned)
                except:
                    pass
            
            # Fallback to simple field matching
            if ':' in cleaned:
                parts = cleaned.split(':')
                if len(parts) >= 2:
                    field = parts[0].strip().strip("'\"")
                    value = parts[1].strip().strip("'\"")
                    return {field: value}
            
            return {}
            
        except Exception:
            return {}
    
    def _parse_aggregation_pipeline(self, pipeline_str: str) -> List[Dict[str, Any]]:
        """Simple aggregation pipeline parser"""
        try:
            # Try to evaluate as Python list (be careful with this in production!)
            return eval(pipeline_str)
        except Exception:
            # Fallback to simple pipeline
            return [{"$match": {}}]
    
    def _analyze_query_statically(self, query: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Analyze query for obvious performance issues without running explain"""
        issues = []
        query_str = query.get('query', '')
        line_number = query.get('line_number', 0)
        operation = query.get('operation', '')
        
        # Check for empty queries (no filters)
        if query_str.strip() in ['{}', '']:
            issues.append({
                'severity': 'HIGH',
                'message': f"Empty query detected on line {line_number} - will scan entire collection",
                'query': query,
                'type': 'EMPTY_QUERY'
            })
        
        # Check for regex queries without text indexes
        if 'regex' in query_str.lower() and 'text' not in query_str.lower():
            issues.append({
                'severity': 'MEDIUM',
                'message': f"Regex query detected on line {line_number} without text index - may be slow",
                'query': query,
                'type': 'REGEX_WITHOUT_INDEX'
            })
        
        # Check for queries that might scan large datasets
        if 'created_at' in query_str and 'gte' in query_str:
            # Date range queries without proper index
            issues.append({
                'severity': 'MEDIUM',
                'message': f"Date range query on line {line_number} - ensure index on created_at field",
                'query': query,
                'type': 'DATE_RANGE_QUERY'
            })
        
        # Check for complex aggregation without proper indexes
        if operation == 'aggregate' and 'sort' in query_str and 'limit' not in query_str:
            issues.append({
                'severity': 'MEDIUM',
                'message': f"Sort operation without limit on line {line_number} - may be expensive",
                'query': query,
                'type': 'SORT_WITHOUT_LIMIT'
            })
        
        # Check for queries that might benefit from compound indexes
        if query_str.count(':') > 2 and 'compound' not in query_str.lower():
            # Multiple field queries might benefit from compound indexes
            issues.append({
                'severity': 'LOW',
                'message': f"Multi-field query on line {line_number} - consider compound index",
                'query': query,
                'type': 'MULTI_FIELD_QUERY'
            })
        
        return issues
    
    def lint_pr(self, diff_content: str) -> Dict[str, Any]:
        """Main linting function for PR analysis against client data"""
        print("🔍 Analyzing PR for MongoDB queries against client data...")
        
        # Extract queries from diff
        queries = self.extract_queries_from_diff(diff_content)
        print(f"📝 Found {len(queries)} MongoDB queries")
        
        if not queries:
            return {
                'success': True,
                'message': 'No MongoDB queries found in PR',
                'queries': [],
                'issues': []
            }
        
        # Analyze each query
        analyses = []
        issues = []
        
        for query in queries:
            print(f"\n🔍 Analyzing query on line {query['line_number']}: {query['operation']}")
            
            if query['collection'] != 'unknown':
                analysis = self.analyze_query_performance(
                    query['collection'], 
                    query['query'], 
                    query['operation']
                )
                analyses.append(analysis)
                
                # Check for performance issues
                # First, do static analysis
                static_issues = self._analyze_query_statically(query)
                issues.extend(static_issues)
                
                # Then check explain plan results
                if analysis.get('is_collection_scan'):
                    issues.append({
                        'severity': 'HIGH',
                        'message': f"Collection scan detected on line {query['line_number']}",
                        'query': query,
                        'analysis': analysis
                    })
                
                if analysis.get('execution_time_ms', 0) > 100:
                    issues.append({
                        'severity': 'MEDIUM',
                        'message': f"Slow query detected on line {query['line_number']} ({analysis['execution_time_ms']}ms)",
                        'query': query,
                        'analysis': analysis
                    })
                
                if analysis.get('documents_examined', 0) > 1000:
                    issues.append({
                        'severity': 'MEDIUM',
                        'message': f"Large document scan on line {query['line_number']} ({analysis['documents_examined']} docs)",
                        'query': query,
                        'analysis': analysis
                    })
            else:
                print(f"⚠️  Could not determine collection for query on line {query['line_number']}")
        
        # Generate report
        report = {
            'success': len(issues) == 0,
            'total_queries': len(queries),
            'queries_analyzed': len(analyses),
            'issues_found': len(issues),
            'queries': queries,
            'analyses': analyses,
            'issues': issues
        }
        
        return report
    
    def close(self):
        """Close the connection"""
        if self.client:
            self.client.close()
            print("🔌 Connection to client MongoDB closed")


def main():
    """Main CLI function"""
    parser = argparse.ArgumentParser(description='Client MongoDB Performance Linter')
    parser.add_argument('--diff-file', '-f', help='Path to PR diff file')
    parser.add_argument('--diff-content', '-c', help='PR diff content as string')
    parser.add_argument('--fail-on-issues', action='store_true', help='Exit with error code 1 if issues found')
    parser.add_argument('--test', action='store_true', help='Run with built-in test diff')
    
    args = parser.parse_args()
    
    try:
        # Initialize linter
        print("🔗 Initializing client linter...")
        linter = ClientLinter()
        
        # Get diff content
        if args.diff_file:
            if not os.path.exists(args.diff_file):
                print(f"❌ Diff file not found: {args.diff_file}")
                sys.exit(1)
            
            with open(args.diff_file, 'r', encoding='utf-8') as f:
                diff_content = f.read()
            print(f"📄 Reading diff from file: {args.diff_file}")
            
        elif args.diff_content:
            diff_content = args.diff_content
            print("📄 Using provided diff content")
            
        elif args.test:
            # Use built-in test diff
            diff_content = """
diff --git a/app.py b/app.py
index 123..456 100644
--- a/app.py
+++ b/app.py
@@ -15,7 +15,7 @@ def get_users():
-    return db.users.find({'status': 'active'})
+    return db.users.find({'status': 'active', 'role': 'user'})
@@ -25,8 +25,8 @@ def get_products():
-    return db.products.find({'category': 'electronics'})
+    return db.products.find({'category': 'electronics', 'price': {'$lt': 1000}})
@@ -35,5 +35,5 @@ def get_orders():
-    return db.users.find({'user_id': user_id})
+    return db.orders.aggregate([{'$match': {'user_id': user_id}}, {'$sort': {'created_at': -1}}])
"""
            print("🧪 Using built-in test diff")
            
        else:
            print("❌ Please provide either --diff-file, --diff-content, or --test")
            parser.print_help()
            sys.exit(1)
        
        # Run linting
        print("🔍 Running PR analysis...")
        report = linter.lint_pr(diff_content)
        
        # Display results
        print(f"\n📊 Linting Results:")
        print(f"  ✅ Success: {report['success']}")
        print(f"  📝 Queries found: {report['total_queries']}")
        print(f"  🔍 Queries analyzed: {report['queries_analyzed']}")
        print(f"  ⚠️  Issues found: {report['issues_found']}")
        
        if report['queries']:
            print(f"\n📋 Queries detected:")
            for query in report['queries']:
                print(f"  Line {query['line_number']}: {query['operation']} on {query['collection']}")
                print(f"    Query: {query['query']}")
        
        if report['issues']:
            print(f"\n⚠️  Performance Issues:")
            for issue in report['issues']:
                print(f"  [{issue['severity']}] {issue['message']}")
        
        # Check exit conditions
        if args.fail_on_issues and report['issues_found'] > 0:
            print(f"\n❌ Found {report['issues_found']} issues. Exiting with error code 1.")
            sys.exit(1)
        
        if report['success']:
            print("\n✅ PR linting passed! No performance issues found.")
        else:
            print(f"\n⚠️  PR linting completed with {report['issues_found']} issues.")
        
        linter.close()
        
    except Exception as e:
        print(f"❌ Linting failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
