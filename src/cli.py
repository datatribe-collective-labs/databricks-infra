#!/usr/bin/env python3
"""
CLI tool for managing Databricks users and infrastructure.

Usage:
    poetry run user-add
    poetry run user-list
    poetry run user-remove
    poetry run user-status
"""

import json
import sys
from pathlib import Path
from typing import Dict, List

import click


def get_users_file() -> Path:
    """Get the path to the users configuration file (prioritizes users.local.json)."""
    terraform_dir = Path(__file__).parent.parent / "terraform"
    users_local = terraform_dir / "users.local.json"
    users_default = terraform_dir / "users.json"

    return users_local if users_local.exists() else users_default


def load_users() -> Dict:
    """Load users from the configuration file."""
    users_file = get_users_file()
    if not users_file.exists():
        click.echo(f"❌ Users file not found: {users_file}", err=True)
        sys.exit(1)

    with open(users_file, 'r') as f:
        return json.load(f)


def save_users(data: Dict) -> None:
    """Save users to the configuration file with pretty formatting."""
    users_file = get_users_file()
    with open(users_file, 'w') as f:
        json.dump(data, f, indent=2)
    click.echo(f"✅ Saved to {users_file}")


def validate_email(email: str) -> bool:
    """Basic email validation."""
    return '@' in email and '.' in email.split('@')[1]


def get_schema_name(email: str) -> str:
    """Generate schema name from email (same logic as Terraform)."""
    return email.split('@')[0].replace('.', '_').lower()


@click.group()
def cli():
    """Databricks Infrastructure Management CLI"""
    pass


@cli.command()
def list_users():
    """List all users in the configuration."""
    data = load_users()
    users = data.get('users', [])

    click.echo(f"\n📊 Total Users: {len(users)}")
    click.echo(f"📁 Config File: {get_users_file()}\n")
    click.echo(f"{'Email':<45} {'Display Name':<30} {'Groups':<15}")
    click.echo("=" * 90)

    for user in sorted(users, key=lambda x: x['user_name']):
        email = user['user_name']
        name = user['display_name']
        groups = ', '.join(user['groups'])
        click.echo(f"{email:<45} {name:<30} {groups:<15}")

    click.echo("")


@cli.command()
@click.option('--email', prompt='Email address', help='User email address')
@click.option('--name', prompt='Display name', help='User display name')
@click.option('--group',
              type=click.Choice(['admins', 'students'], case_sensitive=False),
              prompt='Group (admins/students)',
              help='User group membership')
@click.option('--apply', is_flag=True, help='Automatically run terraform apply after adding user')
def add_user(email: str, name: str, group: str, apply: bool):
    """Add a new user to the configuration."""

    # Validate email
    if not validate_email(email):
        click.echo(f"❌ Invalid email format: {email}", err=True)
        sys.exit(1)

    # Load existing users
    data = load_users()
    users = data.get('users', [])

    # Check for duplicates
    if any(u['user_name'] == email for u in users):
        click.echo(f"❌ User {email} already exists!", err=True)
        sys.exit(1)

    # Show what will be created
    schema_name = get_schema_name(email)
    click.echo(f"\n📝 User will be created with:")
    click.echo(f"   Email: {email}")
    click.echo(f"   Name: {name}")
    click.echo(f"   Group: platform_{group}")
    click.echo(f"   Schema: databricks_course.{schema_name}")

    # Confirm
    if not click.confirm('\nAdd this user?', default=True):
        click.echo("Cancelled.")
        return

    # Add new user
    new_user = {
        "user_name": email,
        "display_name": name,
        "groups": [group]
    }

    users.append(new_user)
    data['users'] = users

    # Save
    save_users(data)

    click.echo(f"\n✅ Added user: {email}")

    # Optionally run terraform
    if apply:
        click.echo("\n🚀 Running terraform apply...")
        import subprocess

        terraform_dir = Path(__file__).parent.parent / "terraform"
        result = subprocess.run(
            [
                'terraform', 'apply',
                '-var=create_users=true',
                '-var=create_groups=true',
                '-var=create_catalogs=true',
                '-var=create_schemas=true',
                '-auto-approve'
            ],
            cwd=terraform_dir
        )

        if result.returncode == 0:
            click.echo("\n✅ Terraform deployment completed successfully!")
        else:
            click.echo("\n❌ Terraform apply failed!", err=True)
            sys.exit(1)
    else:
        click.echo("\n📌 Next steps:")
        click.echo("   cd terraform")
        click.echo("   terraform plan -var='create_users=true' -var='create_groups=true' -var='create_catalogs=true' -var='create_schemas=true'")
        click.echo("   terraform apply -var='create_users=true' -var='create_groups=true' -var='create_catalogs=true' -var='create_schemas=true'")


@cli.command()
@click.option('--emails', prompt='Email addresses (comma-separated)',
              help='Comma-separated list of email addresses')
@click.option('--apply', is_flag=True, help='Automatically run terraform apply after adding users')
def add_users_batch(emails: str, apply: bool):
    """Add multiple users from a comma-separated list."""

    email_list = [e.strip() for e in emails.split(',') if e.strip()]

    if not email_list:
        click.echo("❌ No valid emails provided", err=True)
        sys.exit(1)

    click.echo(f"\n📋 Adding {len(email_list)} users...\n")

    added_users = []

    for email in email_list:
        if not validate_email(email):
            click.echo(f"⚠️  Skipping invalid email: {email}")
            continue

        # Load fresh data each time
        data = load_users()
        users = data.get('users', [])

        if any(u['user_name'] == email for u in users):
            click.echo(f"⚠️  User {email} already exists, skipping...")
            continue

        name = click.prompt(f"Display name for {email}")
        group = click.prompt(
            f"Group for {email}",
            type=click.Choice(['admins', 'students'], case_sensitive=False)
        )

        users.append({
            "user_name": email,
            "display_name": name,
            "groups": [group]
        })
        data['users'] = users
        save_users(data)

        added_users.append(email)
        click.echo(f"✅ Added {email}\n")

    if not added_users:
        click.echo("\n⚠️  No users were added")
        return

    click.echo(f"\n✅ Successfully added {len(added_users)} user(s)")

    if apply:
        click.echo("\n🚀 Running terraform apply...")
        import subprocess

        terraform_dir = Path(__file__).parent.parent / "terraform"
        result = subprocess.run(
            [
                'terraform', 'apply',
                '-var=create_users=true',
                '-var=create_groups=true',
                '-var=create_catalogs=true',
                '-var=create_schemas=true',
                '-auto-approve'
            ],
            cwd=terraform_dir
        )

        if result.returncode == 0:
            click.echo("\n✅ Deployment completed!")
        else:
            click.echo("\n❌ Deployment failed!", err=True)
            sys.exit(1)


@cli.command()
@click.option('--email', prompt='Email address to remove', help='User email to remove')
@click.option('--yes', is_flag=True, help='Skip confirmation prompt')
def remove_user(email: str, yes: bool):
    """Remove a user from the configuration.

    ⚠️  WARNING: This will also delete their personal schema and all data when terraform apply runs!
    """

    data = load_users()
    users = data.get('users', [])

    # Find user
    user = next((u for u in users if u['user_name'] == email), None)
    if not user:
        click.echo(f"❌ User {email} not found!", err=True)
        sys.exit(1)

    schema_name = get_schema_name(email)

    # Show what will be deleted
    click.echo(f"\n⚠️  WARNING: This will remove:")
    click.echo(f"   User: {user['display_name']} ({email})")
    click.echo(f"   Schema: databricks_course.{schema_name}")
    click.echo(f"   All tables and data in that schema")

    # Confirm deletion
    if not yes:
        if not click.confirm('\n❗ Are you absolutely sure you want to continue?', default=False):
            click.echo("Cancelled.")
            return

    # Remove user
    data['users'] = [u for u in users if u['user_name'] != email]
    save_users(data)

    click.echo(f"\n✅ Removed {email} from configuration")
    click.echo("\n📌 Next step:")
    click.echo("   cd terraform")
    click.echo("   terraform apply -var='create_users=true' -var='create_groups=true' -var='create_catalogs=true' -var='create_schemas=true'")
    click.echo("\n⚠️  This will permanently delete the user and their data from Databricks!")


@cli.command()
def status():
    """Show infrastructure status summary."""
    data = load_users()
    users = data.get('users', [])

    admins = [u for u in users if 'admins' in u['groups']]
    students = [u for u in users if 'students' in u['groups']]

    click.echo("\n" + "=" * 60)
    click.echo("📊 Databricks Infrastructure Status")
    click.echo("=" * 60)

    click.echo(f"\n👥 Users:")
    click.echo(f"   Total: {len(users)}")
    click.echo(f"   Admins: {len(admins)} (platform_admins group)")
    click.echo(f"   Students: {len(students)} (platform_students group)")

    click.echo(f"\n📁 Unity Catalog Structure:")
    click.echo(f"   Catalogs: 5 total")
    click.echo(f"     - 4 shared reference (sales_dev, sales_prod, marketing_dev, marketing_prod)")
    click.echo(f"     - 1 course catalog (databricks_course)")

    click.echo(f"\n   Schemas in databricks_course:")
    click.echo(f"     - Shared: 3 (shared_bronze, shared_silver, shared_gold)")
    click.echo(f"     - User-specific: {len(users)}")
    click.echo(f"     - Total: {len(users) + 3}")

    click.echo(f"\n📚 Course Content:")
    click.echo(f"   Notebooks: 27")
    click.echo(f"   Modules:")
    click.echo(f"     - Week 1: Databricks Fundamentals (5 notebooks)")
    click.echo(f"     - Foundations: Data Modelling (4 notebooks)")
    click.echo(f"     - Week 2: Data Ingestion (5 notebooks)")
    click.echo(f"     - Week 3: Transformations (4 notebooks)")
    click.echo(f"     - Week 4: End-to-End Workflows (3 notebooks)")
    click.echo(f"     - Week 5: Production Deployment (4 notebooks)")
    click.echo(f"     - Advanced: Databricks Apps (2 notebooks)")

    click.echo(f"\n🌐 Workspace:")
    click.echo(f"   URL: https://dbc-d8111651-e8b1.cloud.databricks.com")
    click.echo(f"   Notebooks Path: /Shared/terraform-managed/course/notebooks/")

    click.echo(f"\n📝 Configuration:")
    click.echo(f"   Users File: {get_users_file()}")
    click.echo(f"   Terraform Dir: {Path(__file__).parent.parent / 'terraform'}")

    click.echo("=" * 60 + "\n")


# Entry points for poetry scripts
def add_user_cmd():
    """Entry point for user-add command."""
    cli(['add-user'])


def list_users_cmd():
    """Entry point for user-list command."""
    cli(['list-users'])


def remove_user_cmd():
    """Entry point for user-remove command."""
    cli(['remove-user'])


def status_cmd():
    """Entry point for user-status command."""
    cli(['status'])


def add_batch_cmd():
    """Entry point for user-add-batch command."""
    cli(['add-users-batch'])


if __name__ == '__main__':
    cli()