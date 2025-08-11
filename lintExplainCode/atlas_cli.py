#!/usr/bin/env python3
"""
Atlas CLI - Simple command-line interface for MongoDB performance linting
"""

import click
import sys
from pathlib import Path
from atlas_linter import AtlasLinter


@click.group()
def cli():
    """Atlas MongoDB Performance Linter"""
    pass


@cli.command()
@click.option('--diff-file', '-f', help='Path to PR diff file')
@click.option('--diff-content', '-c', help='PR diff content as string')
@click.option('--fail-on-issues', is_flag=True, help='Exit with error code 1 if issues found')
def lint_pr(diff_file, diff_content, fail_on_issues):
    """Lint MongoDB queries in a PR diff"""
    try:
        # Get diff content
        if diff_file:
            if not Path(diff_file).exists():
                click.echo(f"❌ Diff file not found: {diff_file}", err=True)
                sys.exit(1)
            
            with open(diff_file, 'r', encoding='utf-8') as f:
                diff_content = f.read()
            click.echo(f"📄 Reading diff from file: {diff_file}")
        
        elif diff_content:
            click.echo("📄 Using provided diff content")
        else:
            click.echo("❌ Either --diff-file or --diff-content must be provided", err=True)
            sys.exit(1)
        
        # Initialize linter
        click.echo("🔗 Initializing Atlas linter...")
        linter = AtlasLinter()
        
        # Run linting
        click.echo("🔍 Running PR analysis...")
        report = linter.lint_pr(diff_content)
        
        # Display results
        click.echo(f"\n📊 Linting Results:")
        click.echo(f"  ✅ Success: {report['success']}")
        click.echo(f"  📝 Queries found: {report['total_queries']}")
        click.echo(f"  🔍 Queries analyzed: {report['queries_analyzed']}")
        click.echo(f"  ⚠️  Issues found: {report['issues_found']}")
        
        if report['queries']:
            click.echo(f"\n📋 Queries detected:")
            for query in report['queries']:
                click.echo(f"  Line {query['line_number']}: {query['operation']} on {query['collection']}")
                click.echo(f"    Query: {query['query']}")
        
        if report['issues']:
            click.echo(f"\n⚠️  Performance Issues:")
            for issue in report['issues']:
                click.echo(f"  [{issue['severity']}] {issue['message']}")
        
        # Check exit conditions
        if fail_on_issues and report['issues_found'] > 0:
            click.echo(f"\n❌ Found {report['issues_found']} issues. Exiting with error code 1.")
            sys.exit(1)
        
        if report['success']:
            click.echo("\n✅ PR linting passed! No performance issues found.")
        else:
            click.echo(f"\n⚠️  PR linting completed with {report['issues_found']} issues.")
        
        linter.close()
        
    except Exception as e:
        click.echo(f"❌ Linting failed: {e}", err=True)
        sys.exit(1)


@cli.command()
def test():
    """Test the Atlas linter with sample data"""
    try:
        click.echo("🧪 Testing Atlas linter...")
        linter = AtlasLinter()
        
        # Test with sample diff
        sample_diff = """
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
-    return db.orders.find({'user_id': user_id})
+    return db.orders.aggregate([{'$match': {'user_id': user_id}}, {'$sort': {'created_at': -1}}])
"""
        
        report = linter.lint_pr(sample_diff)
        
        click.echo(f"\n📊 Test Results:")
        click.echo(f"  Success: {report['success']}")
        click.echo(f"  Issues: {report['issues_found']}")
        
        linter.close()
        click.echo("✅ Test completed!")
        
    except Exception as e:
        click.echo(f"❌ Test failed: {e}", err=True)
        sys.exit(1)


@cli.command()
def collections():
    """List collections and indexes in the database"""
    try:
        click.echo("🔗 Connecting to Atlas...")
        linter = AtlasLinter()
        
        collections = linter.db.list_collection_names()
        click.echo(f"\n📚 Collections in database:")
        
        for collection_name in collections:
            collection = linter.db[collection_name]
            indexes = list(collection.list_indexes())
            index_names = [idx['name'] for idx in indexes if idx['name'] != '_id_']
            
            click.echo(f"\n  📊 {collection_name}:")
            click.echo(f"    Indexes: {index_names}")
            
            # Count documents
            doc_count = collection.count_documents({})
            click.echo(f"    Documents: {doc_count}")
        
        linter.close()
        
    except Exception as e:
        click.echo(f"❌ Failed to list collections: {e}", err=True)
        sys.exit(1)


if __name__ == '__main__':
    cli()
