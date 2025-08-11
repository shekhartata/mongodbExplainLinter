# Atlas MongoDB Performance Linter

Simple CI/CD tool that analyzes PR queries and runs MongoDB explain plans with executionStats.

## 🚀 Quick Start

### 1. Setup Atlas Connection
```bash
python setup.py
```
Enter your Atlas connection string when prompted.

### 2. Test the Linter
```bash
python atlas_cli.py test
```

### 3. View Collections
```bash
python atlas_cli.py collections
```

## 🔍 Lint a PR

### From a diff file:
```bash
python atlas_cli.py lint-pr -f path/to/diff.diff
```

### From inline diff content:
```bash
python atlas_cli.py lint-pr -c "diff content here"
```

### Fail CI if issues found:
```bash
python atlas_cli.py lint-pr -f diff.diff --fail-on-issues
```

## 📋 What It Does

1. **Connects to your Atlas cluster**
2. **Creates sample collections** with realistic data and indexes if none exist
3. **Extracts MongoDB queries** from PR diffs
4. **Runs explain plans** with executionStats on each query
5. **Detects performance issues**:
   - Collection scans (HIGH severity)
   - Slow queries > 100ms (MEDIUM severity)
   - Large document scans > 1000 docs (MEDIUM severity)
   - Regex queries without indexes (MEDIUM severity)
   - Empty queries (HIGH severity)
   - Multi-field queries needing compound indexes (LOW severity)

## 🗄️ Sample Data Created

- **Users**: username, email, status, role, department
- **Products**: name, category, price, stock, brand, rating
- **Orders**: order_id, user_id, total, status, created_at

Each collection has appropriate indexes for realistic testing.

## 🔧 CI/CD Integration

```yaml
# GitHub Actions example
- name: Lint MongoDB Queries
  run: |
    python atlas_cli.py lint-pr -f ${{ github.event.pull_request.diff_url }} --fail-on-issues
```

## 📁 Project Structure

```
atlas-linter/
├── atlas_linter.py      # Core linting engine
├── atlas_cli.py         # Command-line interface
├── setup.py             # Atlas connection setup
├── config.py            # Configuration management
├── sample_lint_diff.diff # Sample PR diff for testing
├── requirements.txt     # Python dependencies
├── .gitignore          # Git ignore rules
└── README.md           # This file
```

## 💡 Atlas Connection String Format

```
mongodb+srv://username:password@cluster-name.xxxxx.mongodb.net/database?retryWrites=true&w=majority
```

## 🧪 Testing

The linter automatically creates sample data in your Atlas cluster for testing. It will:

1. Connect to your cluster
2. Check if collections exist
3. Create sample collections with indexes if needed
4. Run explain plans on your PR queries

## 🚨 Performance Issues Detected

- **Collection Scans**: Queries without index usage
- **Slow Queries**: Execution time > 100ms
- **Large Scans**: Examining > 1000 documents
- **Regex Queries**: Without proper text indexes
- **Empty Queries**: No filters applied
- **Multi-field Queries**: Needing compound indexes

## 📊 Output

The linter provides:
- Query detection and analysis
- Performance metrics from explain plans
- Severity-based issue reporting
- CI/CD exit codes for automation

## 🔧 Installation

```bash
# Clone the repository
git clone <your-repo>
cd atlas-linter

# Install dependencies
pip install -r requirements.txt

# Setup Atlas connection
python setup.py
```

## 📝 License

This project is open source and available under the MIT License.
