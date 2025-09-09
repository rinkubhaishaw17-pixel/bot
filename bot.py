from discord.ext import commands, tasks
from discord import app_commands
from datetime import datetime, timedelta, timezone
import os
import json
import time
import io
import traceback
import asyncio
import random
import re
import threading
import sqlite3
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, HTTPServer
from dotenv import load_dotenv
import logging
from typing import Optional, Dict, List, Union
import discord
from concurrent.futures import ThreadPoolExecutor

TARGET_VOUCH_CHANNEL_ID = 1413262309106782268 

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- RENDER/REPLIT HEALTH CHECK SERVER ---
class HealthCheckHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/html')
        self.end_headers()
        self.wfile.write(b"<h1>Bot Status: Running</h1><p>All systems operational!</p>")
    
    def log_message(self, format, *args):
        return

def web_server():
    port = int(os.environ.get("PORT", 8080))
    try:
        server = HTTPServer(("0.0.0.0", port), HealthCheckHandler)
        logger.info(f"Health check server running on port {port}")
        server.serve_forever()
    except Exception as e:
        logger.error(f"Failed to start web server: {e}")

# --- DISCORD BOT SETUP ---
load_dotenv()
intents = discord.Intents.default()
intents.guilds = True
intents.members = True
intents.message_content = True
intents.guild_messages = True
intents.dm_messages = True
bot = commands.Bot(
    command_prefix="!",
    intents=intents,
    help_command=None,
    case_insensitive=True,
    strip_after_prefix=True
)

# constants.py or at the top of your cog/views file
TICKET_PANEL_URL = "https://discord.com/channels/<guild_id>/<channel_id>/<message_id>"

# --- CONFIGURABLE CONSTANTS ---
CHANNELS = {
    'LOG': "ticket-logs",
    'TRANSCRIPT': "transcripts", 
    'VOUCH': "vouches",
    'MOD_LOG': "moderation-logs",
    'GIVEAWAY': "giveaways",
    'GIVEAWAY_LOGS': "giveaway-logs",
    'VERIFICATION': "verification",
    'WELCOME': "welcome",
    'RULES': "rules",
    'STATS': "server-stats",
    'INVOICES': "invoices"
}
CATEGORIES = {
    'TICKETS': "🎫 Support Tickets",
    'STATS': "📊 Server Stats"
}
CONFIG = {
    'DATABASE_PATH': "product_keys.db",
    'BACKUP_FOLDER': "backups",
    'MAX_BACKUPS': 5,
    'BACKUP_INTERVAL_HOURS': 6,
    'MAIN_COLOR': 0x00CED1,  # Dark Cyan
    'SUCCESS_COLOR': 0x00FF7F,  # Spring Green
    'ERROR_COLOR': 0xFF4444,    # Red
    'WARNING_COLOR': 0xFFD700    # Gold
}

# Add this after your existing imports and configurations

# --- COMPREHENSIVE SETUP SYSTEM ---

# Define required roles and their configurations
REQUIRED_ROLES = {
    'customer_tiers': {
        '💎 Diamond Customer': {'color': 0xB9F2FF, 'position_priority': 10},
        '🥇 Platinum Customer': {'color': 0xE5E4E2, 'position_priority': 9},
        '🥈 Gold Customer': {'color': 0xFFD700, 'position_priority': 8},
        '🥉 Silver Customer': {'color': 0xC0C0C0, 'position_priority': 7},
        '🔰 Bronze Customer': {'color': 0xCD7F32, 'position_priority': 6},
        '🆕 New Customer': {'color': 0x95A99C, 'position_priority': 5}
    },
    'staff_roles': {
        '👑 Admin': {'color': 0xFF0000, 'position_priority': 100, 'permissions': discord.Permissions(administrator=True)},
        '🛡️ Moderator': {'color': 0x3498DB, 'position_priority': 90, 'permissions': discord.Permissions(
            kick_members=True, ban_members=True, manage_messages=True, 
            manage_channels=True, manage_nicknames=True, moderate_members=True
        )},
        '🎫 Support Team': {'color': 0x2ECC71, 'position_priority': 80, 'permissions': discord.Permissions(
            manage_messages=True, embed_links=True, attach_files=True,
            read_message_history=True, use_external_emojis=True
        )},
        '✅ Verified': {'color': 0x00FF00, 'position_priority': 4},
        '👤 Member': {'color': 0x7289DA, 'position_priority': 3}
    }
}

# Define channel structures with permissions
CHANNEL_STRUCTURE = {
    '📢 Information': {
        'type': 'category',
        'channels': [
            {'name': 'rules', 'type': 'text', 'read_only': True, 'topic': '📜 Server rules and guidelines'},
            {'name': 'announcements', 'type': 'text', 'read_only': True, 'topic': '📢 Important server announcements'},
            {'name': 'welcome', 'type': 'text', 'read_only': True, 'topic': '👋 Welcome new members!'},
            {'name': 'payment-methods', 'type': 'text', 'read_only': True, 'topic': '💳 Accepted payment methods'},
        ]
    },
    '💬 Community': {
        'type': 'category',
        'channels': [
            {'name': 'general-chat', 'type': 'text', 'read_only': False, 'topic': '💬 General discussions'},
            {'name': 'vouches', 'type': 'text', 'read_only': True, 'topic': '⭐ Customer reviews and vouches'},
            {'name': 'giveaways', 'type': 'text', 'read_only': True, 'topic': '🎁 Server giveaways'},
        ]
    },
    '📦 Products': {
        'type': 'category',
        'channels': [
            {'name': 'product-list', 'type': 'text', 'read_only': True, 'topic': '🛍️ Available products'},
            {'name': 'stock-updates', 'type': 'text', 'read_only': True, 'topic': '📊 Stock availability updates'},
        ]
    },
    '🎫 Support': {
        'type': 'category',
        'channels': [
            {'name': 'create-ticket', 'type': 'text', 'read_only': True, 'topic': '🎫 Click here to create a support ticket'},
            {'name': 'faq', 'type': 'text', 'read_only': True, 'topic': '❓ Frequently asked questions'},
        ]
    },
    '📊 Server Stats': {
        'type': 'category',
        'channels': [
            {'name': '👥 Members: 0', 'type': 'voice', 'user_limit': 0},
            {'name': '🤖 Bots: 0', 'type': 'voice', 'user_limit': 0},
            {'name': '📊 Total: 0', 'type': 'voice', 'user_limit': 0},
        ]
    }
}

class SetupProgressView(discord.ui.View):
    def __init__(self, total_steps: int = 5):
        super().__init__(timeout=300)
        self.current_step = 0
        self.total_steps = total_steps
        self.completed_tasks = []
        self.failed_tasks = []
        
    def create_progress_embed(self, title: str, description: str) -> discord.Embed:
        progress_bar = self.generate_progress_bar()
        
        embed = create_embed(
            title,
            f"{description}\n\n{progress_bar}",
            CONFIG['MAIN_COLOR'] if self.current_step < self.total_steps else CONFIG['SUCCESS_COLOR']
        )
        
        if self.completed_tasks:
            embed.add_field(
                name="✅ Completed",
                value="\n".join(self.completed_tasks[-5:]),  # Show last 5 completed
                inline=False
            )
        
        if self.failed_tasks:
            embed.add_field(
                name="❌ Failed",
                value="\n".join(self.failed_tasks[-3:]),  # Show last 3 failed
                inline=False
            )
        
        embed.set_footer(text=f"Step {self.current_step}/{self.total_steps}")
        return embed
    
    def generate_progress_bar(self) -> str:
        filled = "█" * (self.current_step * 2)
        empty = "░" * ((self.total_steps - self.current_step) * 2)
        percentage = (self.current_step / self.total_steps) * 100
        return f"Progress: [{filled}{empty}] {percentage:.0f}%"

@bot.tree.command(name="setup_server", description="Complete server setup with all roles and channels")
@app_commands.checks.has_permissions(administrator=True)
async def setup_server(interaction: discord.Interaction):
    """Comprehensive server setup command"""
    await interaction.response.defer(ephemeral=False)
    
    guild = interaction.guild
    view = SetupProgressView()
    created_items = {'roles': [], 'channels': [], 'categories': []}
    
    # Initial message
    setup_message = await interaction.followup.send(
        embed=view.create_progress_embed(
            "🚀 Server Setup Started",
            "Initializing comprehensive server setup..."
        ),
        ephemeral=False
    )
    
    # Step 1: Create Staff Roles
    view.current_step = 1
    await setup_message.edit(embed=view.create_progress_embed(
        "👥 Creating Staff Roles",
        "Setting up administrative and support roles..."
    ))
    
    for role_name, config in REQUIRED_ROLES['staff_roles'].items():
        existing_role = discord.utils.get(guild.roles, name=role_name)
        if not existing_role:
            try:
                permissions = config.get('permissions', discord.Permissions())
                new_role = await guild.create_role(
                    name=role_name,
                    color=discord.Color(config['color']),
                    permissions=permissions,
                    mentionable=True,
                    reason="Server setup - Staff role"
                )
                created_items['roles'].append(role_name)
                view.completed_tasks.append(f"Created role: {role_name}")
                
                # Save special roles to config
                if '🎫 Support Team' in role_name:
                    guild_id = str(guild.id)
                    if guild_id not in data_manager.data['ticket_config']:
                        data_manager.data['ticket_config'][guild_id] = {}
                    data_manager.data['ticket_config'][guild_id]['support_role_id'] = new_role.id
                    data_manager.save_category_data('ticket_config')
                    
            except discord.Forbidden:
                view.failed_tasks.append(f"Failed to create: {role_name}")
            except Exception as e:
                logger.error(f"Error creating role {role_name}: {e}")
        else:
            view.completed_tasks.append(f"Role exists: {role_name}")
    
    await asyncio.sleep(1)  # Brief pause for visibility
    
    # Step 2: Create Customer Tier Roles
    view.current_step = 2
    await setup_message.edit(embed=view.create_progress_embed(
        "💎 Creating Customer Tiers",
        "Setting up customer tier roles..."
    ))
    
    for role_name, config in REQUIRED_ROLES['customer_tiers'].items():
        existing_role = discord.utils.get(guild.roles, name=role_name)
        if not existing_role:
            try:
                new_role = await guild.create_role(
                    name=role_name,
                    color=discord.Color(config['color']),
                    mentionable=False,
                    reason="Server setup - Customer tier"
                )
                created_items['roles'].append(role_name)
                view.completed_tasks.append(f"Created tier: {role_name}")
            except discord.Forbidden:
                view.failed_tasks.append(f"Failed to create: {role_name}")
            except Exception as e:
                logger.error(f"Error creating tier {role_name}: {e}")
        else:
            view.completed_tasks.append(f"Tier exists: {role_name}")
    
    await asyncio.sleep(1)
    
    # Step 3: Create Categories and Channels
    view.current_step = 3
    await setup_message.edit(embed=view.create_progress_embed(
        "📁 Creating Categories & Channels",
        "Setting up channel structure with proper permissions..."
    ))
    
    # Get roles for permission overwrites
    admin_role = discord.utils.get(guild.roles, name="👑 Admin")
    mod_role = discord.utils.get(guild.roles, name="🛡️ Moderator")
    support_role = discord.utils.get(guild.roles, name="🎫 Support Team")
    
    for category_name, category_data in CHANNEL_STRUCTURE.items():
        # Check if category exists
        category = discord.utils.get(guild.categories, name=category_name)
        
        if not category:
            try:
                # Create category with base permissions
                overwrites = {
                    guild.default_role: discord.PermissionOverwrite(view_channel=True),
                    guild.me: discord.PermissionOverwrite(
                        view_channel=True,
                        send_messages=True,
                        manage_messages=True,
                        embed_links=True,
                        attach_files=True,
                        read_message_history=True,
                        manage_channels=True
                    )
                }
                
                # Add staff permissions
                if admin_role:
                    overwrites[admin_role] = discord.PermissionOverwrite(
                        view_channel=True,
                        send_messages=True,
                        manage_messages=True,
                        manage_channels=True
                    )
                if mod_role:
                    overwrites[mod_role] = discord.PermissionOverwrite(
                        view_channel=True,
                        send_messages=True,
                        manage_messages=True
                    )
                
                category = await guild.create_category(
                    name=category_name,
                    overwrites=overwrites,
                    reason="Server setup"
                )
                created_items['categories'].append(category_name)
                view.completed_tasks.append(f"Created category: {category_name}")
            except discord.Forbidden:
                view.failed_tasks.append(f"Failed category: {category_name}")
                continue
        
        # Create channels in category
        for channel_info in category_data['channels']:
            channel_name = channel_info['name']
            existing_channel = discord.utils.get(guild.channels, name=channel_name)
            
            if not existing_channel:
                try:
                    # Set up channel-specific permissions
                    channel_overwrites = {
                        guild.default_role: discord.PermissionOverwrite(
                            view_channel=True,
                            send_messages=not channel_info.get('read_only', True),
                            add_reactions=not channel_info.get('read_only', True),
                            create_public_threads=False,
                            create_private_threads=False
                        ),
                        guild.me: discord.PermissionOverwrite(
                            view_channel=True,
                            send_messages=True,
                            manage_messages=True,
                            embed_links=True,
                            attach_files=True,
                            read_message_history=True
                        )
                    }
                    
                    # Add staff permissions for channels
                    if admin_role:
                        channel_overwrites[admin_role] = discord.PermissionOverwrite(
                            view_channel=True,
                            send_messages=True,
                            manage_messages=True
                        )
                    if mod_role:
                        channel_overwrites[mod_role] = discord.PermissionOverwrite(
                            view_channel=True,
                            send_messages=True,
                            manage_messages=True
                        )
                    if support_role and 'support' in channel_name.lower():
                        channel_overwrites[support_role] = discord.PermissionOverwrite(
                            view_channel=True,
                            send_messages=True,
                            manage_messages=True
                        )
                    
                    if channel_info['type'] == 'text':
                        new_channel = await guild.create_text_channel(
                            name=channel_name,
                            category=category,
                            overwrites=channel_overwrites,
                            topic=channel_info.get('topic', ''),
                            reason="Server setup"
                        )
                    elif channel_info['type'] == 'voice':
                        new_channel = await guild.create_voice_channel(
                            name=channel_name,
                            category=category,
                            user_limit=channel_info.get('user_limit', 0),
                            reason="Server setup"
                        )
                        
                        # Save stats channels
                        if 'Members' in channel_name:
                            guild_id = str(guild.id)
                            if guild_id not in data_manager.data['stats_channels']:
                                data_manager.data['stats_channels'][guild_id] = {}
                            data_manager.data['stats_channels'][guild_id]['member_channel'] = new_channel.id
                            data_manager.save_category_data('stats_channels')
                        elif 'Bots' in channel_name:
                            guild_id = str(guild.id)
                            if guild_id not in data_manager.data['stats_channels']:
                                data_manager.data['stats_channels'][guild_id] = {}
                            data_manager.data['stats_channels'][guild_id]['bot_channel'] = new_channel.id
                            data_manager.save_category_data('stats_channels')
                    
                    created_items['channels'].append(channel_name)
                    view.completed_tasks.append(f"Created #{channel_name}")
                    
                except discord.Forbidden:
                    view.failed_tasks.append(f"Failed #{channel_name}")
                except Exception as e:
                    logger.error(f"Error creating channel {channel_name}: {e}")
    
    await asyncio.sleep(1)
    
    # Step 4: Create Private Log Category
    view.current_step = 4
    await setup_message.edit(embed=view.create_progress_embed(
        "🔒 Setting Up Private Logs",
        "Creating secure logging channels..."
    ))
    
    log_category_name = "🔒 Bot Logs"
    log_category = discord.utils.get(guild.categories, name=log_category_name)
    
    if not log_category:
        try:
            # Private category permissions
            log_overwrites = {
                guild.default_role: discord.PermissionOverwrite(view_channel=False),
                guild.me: discord.PermissionOverwrite(view_channel=True, send_messages=True),
            }
            
            if admin_role:
                log_overwrites[admin_role] = discord.PermissionOverwrite(view_channel=True)
            if mod_role:
                log_overwrites[mod_role] = discord.PermissionOverwrite(view_channel=True)
            
            log_category = await guild.create_category(
                name=log_category_name,
                overwrites=log_overwrites,
                reason="Server setup - Private logs"
            )
            
            # Save admin role for logs
            guild_id = str(guild.id)
            if guild_id not in data_manager.data.get('log_config', {}):
                data_manager.data['log_config'][guild_id] = {}
            data_manager.data['log_config'][guild_id]['admin_role_id'] = admin_role.id if admin_role else None
            data_manager.save_category_data('log_config')
            
            view.completed_tasks.append(f"Created private category: {log_category_name}")
        except discord.Forbidden:
            view.failed_tasks.append(f"Failed to create log category")
    
    # Create log channels
    log_channels = [
        CHANNELS['LOG'], CHANNELS['TRANSCRIPT'], CHANNELS['MOD_LOG'],
        CHANNELS['GIVEAWAY_LOGS'], CHANNELS['INVOICES']
    ]
    
    for channel_name in log_channels:
        existing = discord.utils.get(guild.text_channels, name=channel_name)
        if not existing and log_category:
            try:
                await guild.create_text_channel(
                    name=channel_name,
                    category=log_category,
                    reason="Server setup - Log channel"
                )
                created_items['channels'].append(channel_name)
                view.completed_tasks.append(f"Created log: #{channel_name}")
            except:
                view.failed_tasks.append(f"Failed log: #{channel_name}")
    
    await asyncio.sleep(1)
    
    # Step 5: Final Configuration
    view.current_step = 5
    await setup_message.edit(embed=view.create_progress_embed(
        "⚙️ Finalizing Setup",
        "Applying final configurations..."
    ))
    
    # Update stats channels
    if 'stats_channels' in data_manager.data and str(guild.id) in data_manager.data['stats_channels']:
        member_count = guild.member_count
        bot_count = sum(1 for m in guild.members if m.bot)
        human_count = member_count - bot_count
        
        stats_config = data_manager.data['stats_channels'][str(guild.id)]
        if 'member_channel' in stats_config:
            try:
                member_channel = guild.get_channel(stats_config['member_channel'])
                if member_channel:
                    await member_channel.edit(name=f"👥 Members: {human_count}")
            except:
                pass
        
        if 'bot_channel' in stats_config:
            try:
                bot_channel = guild.get_channel(stats_config['bot_channel'])
                if bot_channel:
                    await bot_channel.edit(name=f"🤖 Bots: {bot_count}")
            except:
                pass
    
    view.completed_tasks.append("✅ Setup completed successfully!")
    
    # Create summary embed
    summary_embed = create_embed(
        "✅ Server Setup Complete!",
        f"Successfully configured your server with all required components.",
        CONFIG['SUCCESS_COLOR'],
        fields=[
            ("📝 Created Roles", f"{len(created_items['roles'])} roles", True),
            ("📁 Created Categories", f"{len(created_items['categories'])} categories", True),
            ("💬 Created Channels", f"{len(created_items['channels'])} channels", True),
            ("⚠️ Failed Tasks", f"{len(view.failed_tasks)} items", True),
            ("✅ Success Rate", f"{((view.current_step - len(view.failed_tasks)/10) / view.total_steps * 100):.0f}%", True),
            ("👤 Setup By", interaction.user.mention, True)
        ],
        thumbnail=guild.icon.url if guild.icon else None
    )
    
    if view.failed_tasks:
        summary_embed.add_field(
            name="❌ Failed Items",
            value="\n".join(view.failed_tasks[:5]),
            inline=False
        )
    
    summary_embed.add_field(
        name="📌 Next Steps",
        value=(
            "1. Use `/ticket` to set up the ticket panel in #create-ticket\n"
            "2. Configure welcome messages with `/setup_welcome`\n"
            "3. Add auto-roles with `/add_auto_role`\n"
            "4. Post your products with `/post_product`\n"
            "5. Start giveaways with `/giveaway`"
        ),
        inline=False
    )
    
    # Add management buttons
    class SetupCompleteView(discord.ui.View):
        def __init__(self):
            super().__init__(timeout=300)
        
        @discord.ui.button(label="📋 View Setup Report", style=discord.ButtonStyle.primary)
        async def view_report(self, button_interaction: discord.Interaction, button: discord.ui.Button):
            report_embed = create_embed(
                "📋 Setup Report",
                "Detailed breakdown of the setup process",
                CONFIG['MAIN_COLOR']
            )
            
            if created_items['roles']:
                report_embed.add_field(
                    name="✅ Created Roles",
                    value="\n".join(f"• {r}" for r in created_items['roles'][:10]),
                    inline=False
                )
            
            if created_items['channels']:
                report_embed.add_field(
                    name="✅ Created Channels",
                    value="\n".join(f"• #{c}" for c in created_items['channels'][:10]),
                    inline=False
                )
            
            await button_interaction.response.send_message(embed=report_embed, ephemeral=True)
        
        @discord.ui.button(label="🎫 Setup Ticket Panel", style=discord.ButtonStyle.success)
        async def setup_tickets(self, button_interaction: discord.Interaction, button: discord.ui.Button):
            ticket_channel = discord.utils.get(guild.text_channels, name="create-ticket")
            if ticket_channel:
                await button_interaction.response.send_message(
                    f"Please use `/ticket` in {ticket_channel.mention} to set up the ticket panel.",
                    ephemeral=True
                )
            else:
                await button_interaction.response.send_message(
                    "❌ Ticket channel not found. Please create it first.",
                    ephemeral=True
                )
    
    await setup_message.edit(embed=summary_embed, view=SetupCompleteView())

# Additional command to check server setup status
@bot.tree.command(name="setup_status", description="Check the current server setup status")
@app_commands.checks.has_permissions(manage_guild=True)
async def setup_status(interaction: discord.Interaction):
    """Check what components are already set up"""
    await interaction.response.defer(ephemeral=True)
    
    guild = interaction.guild
    
    # Check roles
    missing_roles = []
    existing_roles = []
    
    for category in REQUIRED_ROLES.values():
        for role_name in category.keys():
            if discord.utils.get(guild.roles, name=role_name):
                existing_roles.append(role_name)
            else:
                missing_roles.append(role_name)
    
    # Check channels
    missing_channels = []
    existing_channels = []
    
    for category_data in CHANNEL_STRUCTURE.values():
        for channel_info in category_data['channels']:
            channel_name = channel_info['name']
            if discord.utils.get(guild.channels, name=channel_name):
                existing_channels.append(channel_name)
            else:
                missing_channels.append(channel_name)
    
    # Create status embed
    status_embed = create_embed(
        "📊 Server Setup Status",
        f"Current configuration status for {guild.name}",
        CONFIG['MAIN_COLOR'] if missing_roles or missing_channels else CONFIG['SUCCESS_COLOR'],
        thumbnail=guild.icon.url if guild.icon else None,
        fields=[
            ("✅ Existing Roles", f"{len(existing_roles)} roles", True),
            ("❌ Missing Roles", f"{len(missing_roles)} roles", True),
            ("✅ Existing Channels", f"{len(existing_channels)} channels", True),
            ("❌ Missing Channels", f"{len(missing_channels)} channels", True),
            ("📊 Setup Completion", f"{((len(existing_roles) + len(existing_channels)) / (len(existing_roles) + len(existing_channels) + len(missing_roles) + len(missing_channels)) * 100):.0f}%", True),
        ]
    )
    
    if missing_roles:
        status_embed.add_field(
            name="❌ Missing Roles",
            value="\n".join(f"• {r}" for r in missing_roles[:5]) + (f"\n... and {len(missing_roles) - 5} more" if len(missing_roles) > 5 else ""),
            inline=False
        )
    
    if missing_channels:
        status_embed.add_field(
            name="❌ Missing Channels", 
            value="\n".join(f"• #{c}" for c in missing_channels[:5]) + (f"\n... and {len(missing_channels) - 5} more" if len(missing_channels) > 5 else ""),
            inline=False
        )
    
    # Add quick setup button if items are missing
    if missing_roles or missing_channels:
        class QuickSetupView(discord.ui.View):
            def __init__(self):
                super().__init__(timeout=60)
            
            @discord.ui.button(label="🚀 Run Setup", style=discord.ButtonStyle.success)
            async def run_setup(self, button_interaction: discord.Interaction, button: discord.ui.Button):
                await button_interaction.response.send_message(
                    "Starting server setup... Please use `/setup_server` to begin the process.",
                    ephemeral=True
                )
        
        await interaction.followup.send(embed=status_embed, view=QuickSetupView(), ephemeral=True)
    else:
        status_embed.description = "✅ Your server is fully configured!"
        await interaction.followup.send(embed=status_embed, ephemeral=True)

# Channel Management System
class ChannelManagementView(discord.ui.View):
    def __init__(self, guild):
        super().__init__(timeout=300)
        self.guild = guild
        self.selected_channels = set()
        self.selected_categories = set()
        self.update_options()

    def update_options(self):
        self.clear_items()
        
        # Channel dropdown
        channel_options = []
        for channel in self.guild.channels:
            if isinstance(channel, (discord.TextChannel, discord.VoiceChannel)):
                channel_options.append(discord.SelectOption(
                    label=f"#{channel.name}" if isinstance(channel, discord.TextChannel) else f"🔊 {channel.name}",
                    value=str(channel.id),
                    description=f"Category: {channel.category.name if channel.category else 'No category'}"
                ))
        
        if channel_options:
            self.add_item(ChannelSelect(channel_options[:25]))
        
        # Category dropdown
        category_options = []
        for category in self.guild.categories:
            channel_count = len(category.channels)
            category_options.append(discord.SelectOption(
                label=f"📁 {category.name}",
                value=str(category.id),
                description=f"{channel_count} channels"
            ))
        
        if category_options:
            self.add_item(CategorySelect(category_options[:25]))
        
        # Action buttons
        self.add_item(DeleteButton())
        self.add_item(DeleteAllButton())  # New button
        self.add_item(ClearSelectionButton())

    def create_management_embed(self):
        embed = discord.Embed(
            title="🗑️ Channel Management",
            description="Select channels and categories to delete, or use **Delete All** to remove everything.\n**These actions cannot be undone!**",
            color=CONFIG['ERROR_COLOR']
        )
        
        # Show server stats
        total_channels = len([c for c in self.guild.channels if isinstance(c, (discord.TextChannel, discord.VoiceChannel))])
        total_categories = len(self.guild.categories)
        
        embed.add_field(
            name="📊 Server Statistics",
            value=f"Total Channels: {total_channels}\nTotal Categories: {total_categories}",
            inline=True
        )
        
        if self.selected_channels:
            channel_names = []
            for channel_id in self.selected_channels:
                channel = self.guild.get_channel(channel_id)
                if channel:
                    channel_names.append(f"#{channel.name}" if isinstance(channel, discord.TextChannel) else f"🔊 {channel.name}")
            embed.add_field(name="Selected Channels", value="\n".join(channel_names), inline=False)
        
        if self.selected_categories:
            category_names = []
            for category_id in self.selected_categories:
                category = self.guild.get_channel(category_id)
                if category:
                    category_names.append(f"📁 {category.name}")
            embed.add_field(name="Selected Categories", value="\n".join(category_names), inline=False)
        
        if not self.selected_channels and not self.selected_categories:
            embed.add_field(
                name="Instructions", 
                value="• Use dropdowns to select specific items\n• Use **Delete All** to remove everything\n• Use **Clear Selection** to reset", 
                inline=False
            )
        
        return embed

class ChannelSelect(discord.ui.Select):
    def __init__(self, options):
        super().__init__(placeholder="Select channels to delete...", options=options, max_values=min(len(options), 25))

    async def callback(self, interaction: discord.Interaction):
        view = self.view
        view.selected_channels.update(int(value) for value in self.values)
        embed = view.create_management_embed()
        await interaction.response.edit_message(embed=embed, view=view)

class CategorySelect(discord.ui.Select):
    def __init__(self, options):
        super().__init__(placeholder="Select categories to delete...", options=options, max_values=min(len(options), 25))

    async def callback(self, interaction: discord.Interaction):
        view = self.view
        view.selected_categories.update(int(value) for value in self.values)
        embed = view.create_management_embed()
        await interaction.response.edit_message(embed=embed, view=view)

class DeleteButton(discord.ui.Button):
    def __init__(self):
        super().__init__(label="🗑️ Delete Selected", style=discord.ButtonStyle.danger)

    async def callback(self, interaction: discord.Interaction):
        view = self.view
        
        if not view.selected_channels and not view.selected_categories:
            await interaction.response.send_message("❌ No channels or categories selected!", ephemeral=True)
            return

        if not interaction.user.guild_permissions.manage_channels:
            await interaction.response.send_message("❌ You don't have permission to manage channels!", ephemeral=True)
            return

        confirm_view = ConfirmDeleteView(view.selected_channels, view.selected_categories, view.guild, delete_all=False)
        
        total_items = len(view.selected_channels) + len(view.selected_categories)
        embed = discord.Embed(
            title="⚠️ Confirm Deletion",
            description=f"Are you sure you want to delete {total_items} selected item(s)?\n**This action cannot be undone!**",
            color=CONFIG['ERROR_COLOR']
        )
        
        await interaction.response.send_message(embed=embed, view=confirm_view, ephemeral=True)

class DeleteAllButton(discord.ui.Button):
    def __init__(self):
        super().__init__(label="💥 Delete ALL", style=discord.ButtonStyle.danger)

    async def callback(self, interaction: discord.Interaction):
        if not interaction.user.guild_permissions.manage_channels:
            await interaction.response.send_message("❌ You don't have permission to manage channels!", ephemeral=True)
            return

        # Get all channels and categories
        all_channels = set()
        all_categories = set()
        
        for channel in interaction.guild.channels:
            if isinstance(channel, (discord.TextChannel, discord.VoiceChannel)):
                all_channels.add(channel.id)
            elif isinstance(channel, discord.CategoryChannel):
                all_categories.add(channel.id)

        if not all_channels and not all_categories:
            await interaction.response.send_message("❌ No channels or categories found to delete!", ephemeral=True)
            return

        # Show warning confirmation
        confirm_view = DeleteAllConfirmView(all_channels, all_categories, interaction.guild)
        
        total_items = len(all_channels) + len(all_categories)
        embed = discord.Embed(
            title="🚨 **DANGER ZONE** 🚨",
            description=f"**YOU ARE ABOUT TO DELETE EVERYTHING!**\n\n"
                       f"This will delete:\n"
                       f"• **{len(all_channels)}** channels\n"
                       f"• **{len(all_categories)}** categories\n\n"
                       f"**THIS ACTION CANNOT BE UNDONE!**\n"
                       f"**ALL MESSAGES WILL BE LOST FOREVER!**",
            color=0xFF0000  # Bright red
        )
        embed.add_field(
            name="⚠️ WARNING",
            value="This will completely wipe your server's channels!\nMake sure you have backups if needed!",
            inline=False
        )
        
        await interaction.response.send_message(embed=embed, view=confirm_view, ephemeral=True)

class ClearSelectionButton(discord.ui.Button):
    def __init__(self):
        super().__init__(label="🔄 Clear Selection", style=discord.ButtonStyle.secondary)

    async def callback(self, interaction: discord.Interaction):
        view = self.view
        view.selected_channels.clear()
        view.selected_categories.clear()
        embed = view.create_management_embed()
        await interaction.response.edit_message(embed=embed, view=view)

class DeleteAllConfirmView(discord.ui.View):
    def __init__(self, all_channels, all_categories, guild):
        super().__init__(timeout=60)
        self.all_channels = all_channels
        self.all_categories = all_categories
        self.guild = guild
        self.confirmation_step = 0

    @discord.ui.button(label="⚠️ I understand the risks", style=discord.ButtonStyle.danger)
    async def first_confirmation(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self.confirmation_step == 0:
            self.confirmation_step = 1
            
            embed = discord.Embed(
                title="🚨 FINAL CONFIRMATION 🚨",
                description=f"**LAST CHANCE TO CANCEL!**\n\n"
                           f"Type **DELETE EVERYTHING** in the next message to confirm.\n"
                           f"This will permanently delete:\n"
                           f"• **{len(self.all_channels)}** channels\n"
                           f"• **{len(self.all_categories)}** categories\n\n"
                           f"**ALL DATA WILL BE LOST FOREVER!**",
                color=0xFF0000
            )
            
            # Clear the view and add new buttons
            self.clear_items()
            self.add_item(FinalConfirmButton(self.all_channels, self.all_categories, self.guild))
            self.add_item(CancelButton())
            
            await interaction.response.edit_message(embed=embed, view=self)

    @discord.ui.button(label="❌ Cancel", style=discord.ButtonStyle.secondary)
    async def cancel_all(self, interaction: discord.Interaction, button: discord.ui.Button):
        embed = discord.Embed(
            title="✅ Deletion Cancelled",
            description="Smart choice! No channels were deleted.",
            color=CONFIG['SUCCESS_COLOR']
        )
        await interaction.response.edit_message(embed=embed, view=None)

class FinalConfirmButton(discord.ui.Button):
    def __init__(self, all_channels, all_categories, guild):
        super().__init__(label="💀 DELETE EVERYTHING", style=discord.ButtonStyle.danger)
        self.all_channels = all_channels
        self.all_categories = all_categories
        self.guild = guild

    async def callback(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        
        # Start deletion process
        await self.perform_mass_deletion(interaction)

    async def perform_mass_deletion(self, interaction):
        deleted_count = 0
        failed_count = 0
        errors = []
        
        progress_message = await interaction.followup.send(
            embed=discord.Embed(
                title="🔄 Mass Deletion In Progress...",
                description="Please wait while all channels and categories are being deleted...",
                color=0xFFA500
            ), 
            ephemeral=True
        )

        try:
            # Delete all regular channels first
            for channel_id in self.all_channels:
                try:
                    channel = self.guild.get_channel(channel_id)
                    if channel:
                        await channel.delete(reason=f"Mass deletion by {interaction.user}")
                        deleted_count += 1
                        logger.info(f"Deleted channel: {channel.name}")
                        await asyncio.sleep(0.5)
                except Exception as e:
                    logger.error(f"Failed to delete channel {channel_id}: {str(e)}")
                    errors.append(f"Channel {channel_id}: {str(e)}")
                    failed_count += 1

            # Delete all categories
            for category_id in self.all_categories:
                try:
                    category = self.guild.get_channel(category_id)
                    if category and isinstance(category, discord.CategoryChannel):
                        for channel in category.channels:
                            try:
                                await channel.delete(reason=f"Mass deletion by {interaction.user}")
                                deleted_count += 1
                                await asyncio.sleep(0.3)
                            except Exception as e:
                                logger.error(f"Failed to delete channel in category: {str(e)}")
                                failed_count += 1
                        
                        await category.delete(reason=f"Mass deletion by {interaction.user}")
                        logger.info(f"Deleted category: {category.name}")
                        await asyncio.sleep(0.5)
                        
                except Exception as e:
                    logger.error(f"Failed to delete category {category_id}: {str(e)}")
                    errors.append(f"Category {category_id}: {str(e)}")
                    failed_count += 1

        except Exception as e:
            logger.error(f"General error during mass deletion: {str(e)}")
            embed = discord.Embed(
                title="❌ Mass Deletion Failed",
                description=f"An error occurred: {str(e)}",
                color=CONFIG['ERROR_COLOR']
            )
            await progress_message.edit(embed=embed)
            return

        # Send final results
        if deleted_count > 0:
            embed = discord.Embed(
                title="💀 Mass Deletion Complete",
                description=f"**Successfully deleted {deleted_count} items.**\n\nYour server has been wiped clean!",
                color=CONFIG['SUCCESS_COLOR']
            )
            
            if failed_count > 0:
                embed.add_field(
                    name="⚠️ Some items failed",
                    value=f"{failed_count} item(s) could not be deleted.",
                    inline=False
                )
                
            await progress_message.edit(embed=embed, view=None)
        else:
            embed = discord.Embed(
                title="❌ Mass Deletion Failed",
                description="No items were deleted. Check console for details.",
                color=CONFIG['ERROR_COLOR']
            )
            await progress_message.edit(embed=embed, view=None)

class CancelButton(discord.ui.Button):
    def __init__(self):
        super().__init__(label="❌ Cancel", style=discord.ButtonStyle.secondary)

    async def callback(self, interaction: discord.Interaction):
        embed = discord.Embed(
            title="✅ Mass Deletion Cancelled",
            description="Wise decision! No channels were deleted.",
            color=CONFIG['SUCCESS_COLOR']
        )
        await interaction.response.edit_message(embed=embed, view=None)

class ConfirmDeleteView(discord.ui.View):
    def __init__(self, selected_channels, selected_categories, guild, delete_all=False):
        super().__init__(timeout=60)
        self.selected_channels = selected_channels
        self.selected_categories = selected_categories
        self.guild = guild
        self.delete_all = delete_all

    @discord.ui.button(label="✅ Yes, Delete", style=discord.ButtonStyle.danger)
    async def confirm_delete(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True)
        
        deleted_count = 0
        failed_count = 0
        errors = []

        try:
            # Delete channels
            for channel_id in self.selected_channels:
                try:
                    channel = self.guild.get_channel(channel_id)
                    if channel:
                        await channel.delete(reason=f"Bulk deletion by {interaction.user}")
                        deleted_count += 1
                        await asyncio.sleep(0.3)
                except Exception as e:
                    errors.append(f"Channel {channel_id}: {str(e)}")
                    failed_count += 1

            # Delete categories
            for category_id in self.selected_categories:
                try:
                    category = self.guild.get_channel(category_id)
                    if category and isinstance(category, discord.CategoryChannel):
                        for channel in category.channels:
                            try:
                                await channel.delete(reason=f"Category deletion by {interaction.user}")
                                deleted_count += 1
                                await asyncio.sleep(0.3)
                            except Exception as e:
                                failed_count += 1
                        
                        await category.delete(reason=f"Bulk deletion by {interaction.user}")
                        await asyncio.sleep(0.3)
                except Exception as e:
                    errors.append(f"Category {category_id}: {str(e)}")
                    failed_count += 1

        except Exception as e:
            await interaction.followup.send(f"❌ An error occurred: {str(e)}", ephemeral=True)
            return

        # Send results
        if deleted_count > 0:
            embed = discord.Embed(
                title="✅ Deletion Complete",
                description=f"Successfully deleted {deleted_count} item(s).",
                color=CONFIG['SUCCESS_COLOR']
            )
            
            if failed_count > 0:
                embed.add_field(
                    name="⚠️ Some items failed",
                    value=f"{failed_count} item(s) could not be deleted.",
                    inline=False
                )
            
            await interaction.followup.send(embed=embed, ephemeral=True)
        else:
            await interaction.followup.send("❌ No items were deleted.", ephemeral=True)

    @discord.ui.button(label="❌ Cancel", style=discord.ButtonStyle.secondary)
    async def cancel_delete(self, interaction: discord.Interaction, button: discord.ui.Button):
        embed = discord.Embed(
            title="❌ Deletion Cancelled",
            description="No channels were deleted.",
            color=CONFIG['SUCCESS_COLOR']
        )
        await interaction.response.edit_message(embed=embed, view=None)

# === SQLite Key Management System ===
def init_key_database():
    conn = sqlite3.connect(CONFIG['DATABASE_PATH'])
    cursor = conn.cursor()
    
    # Products table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS products (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE NOT NULL,
            description TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # Keys table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS keys (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_name TEXT NOT NULL,
            key_value TEXT NOT NULL UNIQUE,
            used BOOLEAN DEFAULT FALSE,
            user_tag TEXT DEFAULT NULL,
            user_id INTEGER DEFAULT NULL,
            date_used TIMESTAMP DEFAULT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (product_name) REFERENCES products(name)
        )
    ''')
    
    # User purchases tracking
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS user_purchases (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            user_tag TEXT NOT NULL,
            product_name TEXT NOT NULL,
            amount_spent REAL DEFAULT 0.0,
            purchase_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            transaction_id TEXT UNIQUE
        )
    ''')
    
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_keys_product ON keys(product_name);')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_keys_used ON keys(used);')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_keys_user ON keys(user_id);')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_purchases_user ON user_purchases(user_id);')
    
    conn.commit()
    conn.close()
    logger.info("✅ SQLite key database initialized")

@contextmanager
def get_db_connection():
    conn = sqlite3.connect(CONFIG['DATABASE_PATH'])
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()

class KeyManager:
    @staticmethod
    async def add_product(product_name: str, description: str = "") -> bool:
        def sync_add():
            with get_db_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "INSERT OR IGNORE INTO products (name, description) VALUES (?, ?)",
                    (product_name.strip(), description)
                )
                conn.commit()
                return cursor.rowcount > 0
        
        try:
            return await bot.loop.run_in_executor(None, sync_add)
        except Exception as e:
            logger.error(f"❌ Error creating product '{product_name}': {e}")
            return False

    @staticmethod
    async def add_keys_to_product(product_name: str, keys: List[str]) -> Dict[str, Union[bool, str, int]]:
        def sync_add():
            if not keys:
                return {"success": False, "message": "No keys provided", "added": 0, "duplicates": 0}
        
        with get_db_connection() as conn:
            cursor = conn.cursor()
            # Ensure product exists first - sync call within executor
            cursor.execute("INSERT OR IGNORE INTO products (name, description) VALUES (?, ?)", 
                          (product_name.strip(), ""))
            
            added_count = 0
            duplicate_count = 0
            
            for key in keys:
                key = key.strip()
                if not key:
                    continue
                try:
                    cursor.execute(
                        "INSERT INTO keys (product_name, key_value) VALUES (?, ?)",
                        (product_name.strip(), key)
                    )
                    added_count += 1
                except sqlite3.IntegrityError:
                    duplicate_count += 1
                    logger.warning(f"Duplicate key skipped for {product_name}: {key[:10]}...")
            
            conn.commit()
            result_msg = f"Added {added_count} keys to {product_name}"
            if duplicate_count > 0:
                result_msg += f" ({duplicate_count} duplicates skipped)"
            logger.info(f"✅ {result_msg}")
            return {"success": True, "message": result_msg, "added": added_count, "duplicates": duplicate_count}
    
        try:
            return await bot.loop.run_in_executor(None, sync_add)
        except Exception as e:
            logger.error(f"❌ Error adding keys to {product_name}: {e}")
            return {"success": False, "message": f"Database error: {str(e)}", "added": 0, "duplicates": 0}
    
    @staticmethod
    async def use_product_key(product_name: str, user_tag: str, user_id: int, amount_spent: float = 0.0) -> Optional[str]:
        def sync_use():
            with get_db_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT id, key_value FROM keys 
                    WHERE product_name = ? AND used = FALSE 
                    LIMIT 1
                ''', (product_name.strip(),))
                result = cursor.fetchone()
                if not result:
                    return None
                
                key_id, key_value = result['id'], result['key_value']
                cursor.execute('''
                    UPDATE keys 
                    SET used = TRUE, user_tag = ?, user_id = ?, date_used = CURRENT_TIMESTAMP 
                    WHERE id = ?
                ''', (user_tag, user_id, key_id))
                
                # Add to purchase history
                transaction_id = f"txn_{user_id}_{int(datetime.now().timestamp())}"
                cursor.execute('''
                    INSERT INTO user_purchases (user_id, user_tag, product_name, amount_spent, transaction_id)
                    VALUES (?, ?, ?, ?, ?)
                ''', (user_id, user_tag, product_name, amount_spent, transaction_id))
                
                conn.commit()
                return key_value
        
        try:
            return await bot.loop.run_in_executor(None, sync_use)
        except Exception as e:
            logger.error(f"❌ Error using key for {product_name}: {e}")
            return None
    
    @staticmethod
    async def get_product_stock(product_name: str = None) -> Union[int, Dict[str, int]]:
        def sync_get():
            with get_db_connection() as conn:
                cursor = conn.cursor()
                if product_name:
                    cursor.execute('''
                        SELECT COUNT(*) as count FROM keys 
                        WHERE product_name = ? AND used = FALSE
                    ''', (product_name.strip(),))
                    result = cursor.fetchone()
                    return result['count'] if result else 0
                else:
                    cursor.execute('''
                        SELECT product_name, COUNT(*) as count FROM keys 
                        WHERE used = FALSE 
                        GROUP BY product_name
                        ORDER BY product_name
                    ''')
                    results = cursor.fetchall()
                    return {row['product_name']: row['count'] for row in results}
        
        try:
            return await bot.loop.run_in_executor(None, sync_get)
        except Exception as e:
            logger.error(f"❌ Error getting stock: {e}")
            return {} if not product_name else 0
    
    @staticmethod
    async def get_user_purchases_detailed(user_id: int) -> Dict:
        def sync_get():
            with get_db_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT product_name, COUNT(*) as count, SUM(amount_spent) as total_spent,
                           MAX(purchase_date) as last_purchase
                    FROM user_purchases 
                    WHERE user_id = ? 
                    GROUP BY product_name
                    ORDER BY last_purchase DESC
                ''', (user_id,))
                purchases = cursor.fetchall()
                
                cursor.execute('''
                    SELECT COUNT(*) as total_purchases, SUM(amount_spent) as lifetime_spent
                    FROM user_purchases 
                    WHERE user_id = ?
                ''', (user_id,))
                totals = cursor.fetchone()
                
                return {
                    "purchases": [dict(row) for row in purchases],
                    "total_purchases": totals['total_purchases'] or 0,
                    "lifetime_spent": totals['lifetime_spent'] or 0.0
                }
        
        try:
            return await bot.loop.run_in_executor(None, sync_get)
        except Exception as e:
            logger.error(f"❌ Error getting purchases for user {user_id}: {e}")
            return {"purchases": [], "total_purchases": 0, "lifetime_spent": 0.0}

# Initialize database
init_key_database()
key_manager = KeyManager()

class DataManager:
    def __init__(self):
        self.data_files = {
            'warnings': 'warnings.json',
            'auto_roles': 'auto_roles.json',
            'afk': 'afk_status.json',
            'giveaways': 'giveaways.json',
            'templates': 'templates.json',
            'vouches': 'vouch_data.json',
            'welcome': 'welcome_config.json',
            'verification': 'verification_config.json',
            'stats_channels': 'stats_channels.json',
            'dashboard': 'dashboard_config.json',
            'user_profiles': 'user_profiles.json',
            'ticket_config': 'ticket_config.json',
            'dm_templates': 'dm_templates.json',
            'invoices': 'invoices.json',
            'invoice_templates': 'invoice_templates.json',
            'branding': 'branding.json',
            'log_config': 'log_config.json'
        }
        self.data = {}
        self.load_all_data()

        # FIX: Better branding initialization
        self.initialize_branding()

    def initialize_branding(self):
        """Ensure branding data has all required keys"""
        default_branding = {
            'logo_url': "https://media.discordapp.net/attachments/1162388547211370526/1403113823837225082/Copilot_20250805_220724.png?ex=68b7fd54&is=68b6abd4&hm=697f9370d16228dce734cea688dc39c3301bdcd4435bd4e15093b573a18f84b8&=&format=webp&quality=lossless&width=525&height=350",
            'banner_url': "https://media.discordapp.net/attachments/1162388547211370526/1403113823837225082/Copilot_20250805_220724.png?ex=68b7fd54&is=68b6abd4&hm=697f9370d16228dce734cea688dc39c3301bdcd4435bd4e15093b573a18f84b8&=&format=webp&quality=lossless&width=525&height=350",
            'primary_color': CONFIG['MAIN_COLOR'],
            'company_name': "NorthernHub",
            'footer_text': "NorthernHub • Premium Trusted Service"
        }
        
        # Initialize branding if it doesn't exist or is missing keys
        if 'branding' not in self.data or not isinstance(self.data['branding'], dict):
            self.data['branding'] = default_branding.copy()
            self.save_category_data('branding')
        else:
            # Check if all required keys exist, add missing ones
            updated = False
            for key, default_value in default_branding.items():
                if key not in self.data['branding']:
                    self.data['branding'][key] = default_value
                    updated = True
            
            if updated:
                self.save_category_data('branding')

    def load_all_data(self):
        for key, filename in self.data_files.items():
            self.data[key] = self.load_data(filename, {})

    def load_data(self, filename: str, default_value=None) -> dict:
        if default_value is None:
            default_value = {}
            
        if not os.path.exists(filename):
            logger.info(f"Creating new data file: {filename}")
            self.save_data(filename, default_value)
            return default_value
            
        try:
            with open(filename, "r", encoding='utf-8') as f:
                data = json.load(f)
                logger.info(f"Successfully loaded {filename}")
                return data
        except (json.JSONDecodeError, FileNotFoundError) as e:
            logger.error(f"Error loading {filename}: {e}. Using default value.")
            if os.path.exists(filename):
                backup_name = f"{filename}.corrupted.{int(datetime.now().timestamp())}"
                os.rename(filename, backup_name)
                logger.info(f"Backed up corrupted file to {backup_name}")
            return default_value

    def save_data(self, filename: str, data: dict) -> bool:
        try:
            temp_filename = f"{filename}.temp"
            with open(temp_filename, "w", encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            
            if os.path.exists(filename):
                os.replace(temp_filename, filename)
            else:
                os.rename(temp_filename, filename)
            
            return True
        except Exception as e:
            logger.error(f"Error saving {filename}: {e}")
            if os.path.exists(temp_filename):
                os.remove(temp_filename)
            return False

    def save_category_data(self, category: str):
        if category in self.data_files:
            filename = self.data_files[category]
            return self.save_data(filename, self.data[category])
        return False

# Initialize data manager
data_manager = DataManager()



def initialize_default_templates():
    """Initialize templates exactly as shown in screenshots"""
    defaults = {
        "1337": {
            "title": "1337",
            "short_description": "Drücke den Button für mehr Infos!",
            "features": "• Works with RageMP and AltV\n• Regular updates and reliable performance\n• Good functions at affordable price",
            "price": "7 Days RageMP: 4.99€ | 30 Days RageMP: 9.99€ | 90 Days RageMP: 24.99€ | 7 Days AltV: 4.99€ | 30 Days AltV: 9.99€ | 90 Days AltV: not available",
            "image_url": "YOUR_KANACKEN_HUB_GIF_URL_HERE",  # Add your GIF URL
            "color": 0x5865F2,  # Discord blurple
        },
        "Hydrogen": {
            "title": "Hydrogen",
            "short_description": "Drücke den Button für mehr Infos!",
            "features": "• Supports RAGE:MP and AltV\n• User-friendly interface\n• Regular updates",
            "price": "14 Days: 9.99€ | 30 Days: 14.99€ | 90 Days: 29.99€",
            "image_url": "YOUR_KANACKEN_HUB_GIF_URL_HERE",  # Add your GIF URL
            "color": 0x5865F2,
        }
    }
    
    for name, template in defaults.items():
        if name not in data_manager.data['templates']:
            data_manager.data['templates'][name] = template
    
    data_manager.save_category_data('templates')
    logger.info("✅ Default templates initialized")

# Call after DataManager
initialize_default_templates()

initialize_default_templates()
# --- UTILITY FUNCTIONS ---


async def find_or_create_channel(guild: discord.Guild, channel_name: str, category_name: str = None) -> Optional[discord.TextChannel]:
    """
    Finds an existing channel or creates a new one.
    If the channel is a log channel, it will be placed in the private log category.
    """
    log_channel_names = [
        CHANNELS['LOG'], CHANNELS['TRANSCRIPT'], CHANNELS['VOUCH'],
        CHANNELS['MOD_LOG'], CHANNELS['GIVEAWAY_LOGS'], CHANNELS['INVOICES']
    ]
    
    # Determine if this is a log channel
    is_log_channel = channel_name in log_channel_names
    
    # Set up category and permissions for log channels
    if is_log_channel:
        log_category_name = "🔒 Bot Logs"
        target_category = discord.utils.get(guild.categories, name=log_category_name)
        
        # Get the configured admin role
        guild_id = str(guild.id)
        admin_role_id = data_manager.data.get('log_config', {}).get(guild_id, {}).get('admin_role_id')
        admin_role = guild.get_role(admin_role_id) if admin_role_id else None
        
        # Define private permissions
        overwrites = {
            guild.default_role: discord.PermissionOverwrite(view_channel=False),
            guild.me: discord.PermissionOverwrite(view_channel=True)
        }
        if admin_role:
            overwrites[admin_role] = discord.PermissionOverwrite(view_channel=True)
        
        if not target_category:
            try:
                target_category = await guild.create_category(log_category_name, overwrites=overwrites)
                logger.info(f"Created private log category: {log_category_name}")
            except discord.Forbidden:
                logger.error(f"Cannot create private log category '{log_category_name}' in {guild.name}")
                return None
    elif category_name:
        # This is a public channel that needs to be in a specific category
        target_category = discord.utils.get(guild.categories, name=category_name)
        if not target_category:
            try:
                target_category = await guild.create_category(category_name)
            except discord.Forbidden:
                logger.warning(f"Cannot create public category '{category_name}' in {guild.name}")
                target_category = None
        overwrites = {}
    else:
        target_category = None
        overwrites = {}
    
    # Look for existing channel
    channel = discord.utils.get(guild.text_channels, name=channel_name)
    
    if channel:
        # Channel exists - check if it needs to be moved
        if target_category and channel.category != target_category:
            try:
                await channel.edit(category=target_category, overwrites=overwrites if overwrites else None)
                logger.info(f"Moved existing channel #{channel.name} to category '{target_category.name}'")
            except discord.Forbidden:
                logger.error(f"Cannot move channel {channel.name} to {target_category.name}")
        return channel
    
    # Channel doesn't exist, create it
    try:
        new_channel = await guild.create_text_channel(
            channel_name,
            category=target_category,
            overwrites=overwrites if overwrites else {},
            reason="Auto-created by bot"
        )
        logger.info(f"Created channel: #{new_channel.name} in category '{target_category.name if target_category else 'None'}'")
        return new_channel
    except discord.Forbidden:
        logger.error(f"Missing permissions to create channel '{channel_name}' in {guild.name}")
        return None
    except Exception as e:
        logger.error(f"Error creating channel {channel_name}: {e}")
        return None

async def log_to_channel(guild: discord.Guild, message: str, channel_name: str, 
                         embed: discord.Embed = None):
    try:
        channel = await find_or_create_channel(guild, channel_name)
        if not channel:
            logger.warning(f"Could not log to channel {channel_name}")
            return
            
        if embed:
            await channel.send(content=message, embed=embed)
        else:
            await channel.send(message)
            
    except discord.Forbidden:
        logger.error(f"No permission to log to {channel_name}")
    except Exception as e:
        logger.error(f"Error logging to channel {channel_name}: {e}")

async def log_invoice_to_channel(guild: discord.Guild, invoice_data: dict, invoice_embed: discord.Embed):
    """Log invoice creation to the dedicated invoice channel"""
    try:
        invoice_channel = await find_or_create_channel(guild, CHANNELS['INVOICES'])
        if not invoice_channel:
            logger.warning(f"Could not create/find invoice channel in {guild.name}")
            return False
        
        # Create a comprehensive log embed for the invoice channel
        log_embed = create_embed(
            f"💰 New Invoice Generated #{invoice_data['invoice_id']}",
            f"An invoice has been successfully generated and processed.",
            CONFIG['SUCCESS_COLOR'],
            fields=[
                ("📄 Invoice ID", f"#{invoice_data['invoice_id']}", True),
                ("👤 Customer", f"<@{invoice_data['customer_id']}>\n`{invoice_data['customer_tag']}`", True),
                ("📦 Product", invoice_data['product'], True),
                ("💰 Amount", f"${invoice_data.get('amount', 0):.2f}", True),
                ("👨‍💼 Processor", f"<@{invoice_data['processor_id']}>\n`{invoice_data['processor_tag']}`", True),
                ("📅 Generated", f"<t:{invoice_data['timestamp']}:F>", True),
                ("🏷️ Template", invoice_data.get('template_used', 'default'), True),
                ("🆔 Guild ID", f"`{invoice_data['guild_id']}`", True),
                ("🔗 Transaction ID", f"`INV-{invoice_data['invoice_id']}-{invoice_data['timestamp']}`", True)
            ],
            thumbnail=get_branding_data()['logo_url'],  # This makes it circular
            footer=f"Invoice System • {guild.name}"
        )
        
        # Send both the log embed and the actual invoice embed
        await invoice_channel.send(embed=log_embed)
        await invoice_channel.send(content="📋 **Customer Invoice Copy:**", embed=invoice_embed)
        
        # Add a separator for readability
        await invoice_channel.send("─" * 50)
        
        return True
        
    except discord.Forbidden:
        logger.error(f"No permission to log to invoice channel in {guild.name}")
        return False
    except Exception as e:
        logger.error(f"Error logging invoice to channel in {guild.name}: {e}")
        return False

def create_embed(title: str, description: str, color: Union[discord.Color, int] = None, 
                 fields: List[tuple] = None, thumbnail: str = None, 
                 image: str = None, footer: str = None, author: dict = None) -> discord.Embed:
    
    if color is None:
        color = CONFIG['MAIN_COLOR']
    elif isinstance(color, str):
        try:
            color = int(color.replace('#', ''), 16)
        except ValueError:
            color = CONFIG['MAIN_COLOR']
            
    embed = discord.Embed(
        title=title, 
        description=description, 
        color=color, 
        timestamp=datetime.now(timezone.utc)
    )
    
    if fields:
        for name, value, inline in fields:
            # Keep the original value without truncation for formatting
            embed.add_field(name=name, value=str(value), inline=inline)
            
    if thumbnail:
        try:
            embed.set_thumbnail(url=thumbnail)
        except:
            pass
            
    if image:
        try:
            embed.set_image(url=image)
        except:
            pass
            
    if footer:
        embed.set_footer(text=footer)
    else:
        embed.set_footer(text="NorthernHub • Premium Trusted Service")
        
    if author:
        embed.set_author(**author)
        
    return embed

def parse_duration(duration_str: str) -> Optional[timedelta]:
    if not duration_str:
        return None
        
    pattern = r'(?:(\d+)\s*w)?(?:(\d+)\s*d)?(?:(\d+)\s*h)?(?:(\d+)\s*m)?(?:(\d+)\s*s)?'
    match = re.match(pattern, duration_str.strip().lower())
    
    if not match:
        return None
        
    weeks, days, hours, minutes, seconds = [int(x) if x else 0 for x in match.groups()]
    
    if not any([weeks, days, hours, minutes, seconds]):
        return None
        
    return timedelta(
        weeks=weeks, 
        days=days, 
        hours=hours, 
        minutes=minutes, 
        seconds=seconds
    )

def get_customer_tier(total_spent: float) -> tuple:
    if total_spent >= 1000:
        return "💎 Diamond Customer", discord.Color.from_rgb(185, 242, 255)
    elif total_spent >= 500:
        return "🥇 Platinum Customer", discord.Color.from_rgb(229, 228, 226)
    elif total_spent >= 250:
        return "🥈 Gold Customer", discord.Color.from_rgb(255, 215, 0)
    elif total_spent >= 100:
        return "🥉 Silver Customer", discord.Color.from_rgb(192, 192, 192)
    elif total_spent >= 50:
        return "🔰 Bronze Customer", discord.Color.from_rgb(205, 127, 50)
    else:
        return "🆕 New Customer", discord.Color.greyple()

# FIX: Added the missing functions required by the /customer_dashboard command
def get_customer_tier_advanced(total_spent: float, total_purchases: int) -> tuple:
    """Returns tier name, color, and benefits description."""
    if total_spent >= 1000:
        return "💎 Diamond", discord.Color.from_rgb(185, 242, 255), "Priority support, special discounts, exclusive access"
    elif total_spent >= 500:
        return "🥇 Platinum", discord.Color.from_rgb(229, 228, 226), "Priority support, early access"
    elif total_spent >= 250:
        return "🥈 Gold", discord.Color.gold(), "Exclusive giveaway entries"
    elif total_spent >= 100:
        return "🥉 Silver", discord.Color.light_grey(), "Standard support"
    elif total_spent >= 50:
        return "🔰 Bronze", discord.Color.from_rgb(205, 127, 50), "Standard support"
    else:
        return "🆕 New Customer", discord.Color.greyple(), "Basic support"

def get_next_tier_amount(total_spent: float) -> float:
    """Returns the spending amount required for the next tier."""
    if total_spent < 50:
        return 50.0
    elif total_spent < 100:
        return 100.0
    elif total_spent < 250:
        return 250.0
    elif total_spent < 500:
        return 500.0
    elif total_spent < 1000:
        return 1000.0
    else:
        return float('inf')  # Max tier - use inf to indicate no next tier
    
def get_branding_data():
    """Safely get branding data with fallbacks"""
    try:
        branding = data_manager.data.get('branding', {})
        
        # Ensure all required keys exist with fallbacks
        return {
            'logo_url': branding.get('logo_url', "https://via.placeholder.com/150"),
            'banner_url': branding.get('banner_url', "https://via.placeholder.com/800x200"),
            'primary_color': branding.get('primary_color', CONFIG['MAIN_COLOR']),
            'company_name': branding.get('company_name', "NorthernHub"),
            'footer_text': branding.get('footer_text', "NorthernHub • Premium Trusted Service")
        }
    except Exception as e:
        logger.error(f"Error accessing branding data: {e}")
        # Return safe defaults
        return {
            'logo_url': "https://via.placeholder.com/150",
            'banner_url': "https://via.placeholder.com/800x200", 
            'primary_color': CONFIG['MAIN_COLOR'],
            'company_name': "NorthernHub",
            'footer_text': "NorthernHub • Premium Trusted Service"
        }
    

# Function to calculate invoice statistics
async def calculate_invoice_stats(guild_id: str) -> dict:
    """Calculate invoice statistics for a guild"""
    if guild_id not in data_manager.data['invoices']:
        return {
            "total_invoices": 0,
            "total_revenue": 0,
            "avg_invoice_value": 0,
            "invoices_this_month": 0,
            "revenue_this_month": 0,
            "top_products": [],
            "top_customers": []
        }
    
    invoices = data_manager.data['invoices'][guild_id].values()
    
    # Basic stats
    total_invoices = len(invoices)
    total_revenue = sum(invoice.get('amount', 0) for invoice in invoices)
    avg_invoice_value = total_revenue / total_invoices if total_invoices > 0 else 0
    
    # Time-based stats
    now = datetime.now()
    this_month_start = datetime(now.year, now.month, 1, 0, 0, 0)
    this_month_timestamp = int(this_month_start.timestamp())
    
    invoices_this_month = sum(1 for invoice in invoices if invoice.get('timestamp', 0) >= this_month_timestamp)
    revenue_this_month = sum(invoice.get('amount', 0) for invoice in invoices if invoice.get('timestamp', 0) >= this_month_timestamp)
    
    # Product stats
    product_sales = {}
    for invoice in invoices:
        product = invoice.get('product')
        if product:
            if product not in product_sales:
                product_sales[product] = {"count": 0, "revenue": 0}
            product_sales[product]["count"] += 1
            product_sales[product]["revenue"] += invoice.get('amount', 0)
    
    top_products = sorted(
        [{"name": k, "count": v["count"], "revenue": v["revenue"]} for k, v in product_sales.items()],
        key=lambda x: x["revenue"],
        reverse=True
    )[:5]
    
    # Customer stats
    customer_purchases = {}
    for invoice in invoices:
        customer_id = invoice.get('customer_id')
        if customer_id:
            if customer_id not in customer_purchases:
                customer_purchases[customer_id] = {
                    "count": 0, 
                    "revenue": 0, 
                    "tag": invoice.get('customer_tag', f"User {customer_id}")
                }
            customer_purchases[customer_id]["count"] += 1
            customer_purchases[customer_id]["revenue"] += invoice.get('amount', 0)
    
    top_customers = sorted(
        [{"id": k, "tag": v["tag"], "count": v["count"], "revenue": v["revenue"]} for k, v in customer_purchases.items()],
        key=lambda x: x["revenue"],
        reverse=True
    )[:5]
    
    return {
        "total_invoices": total_invoices,
        "total_revenue": total_revenue,
        "avg_invoice_value": avg_invoice_value,
        "invoices_this_month": invoices_this_month,
        "revenue_this_month": revenue_this_month,
        "top_products": top_products,
        "top_customers": top_customers
    }

# Add this after the get_branding_data() function (around line 800-900)

async def setup_bot_permissions(guild):
    """Automatically setup bot permissions when joining a new server"""
    try:
        # Check if bot already has admin permissions
        if guild.me.guild_permissions.administrator:
            logger.info(f"Bot already has admin permissions in {guild.name}")
            return True
        
        # Try to create an admin role for the bot
        admin_role_name = f"{bot.user.name} Admin"
        
        # Check if the role already exists
        existing_role = discord.utils.get(guild.roles, name=admin_role_name)
        
        if not existing_role:
            try:
                # Create admin role with full permissions
                bot_admin_role = await guild.create_role(
                    name=admin_role_name,
                    permissions=discord.Permissions(administrator=True),
                    color=discord.Color.red(),
                    reason="Auto-setup bot admin role",
                    mentionable=False
                )
                
                # Assign the role to the bot
                await guild.me.add_roles(bot_admin_role, reason="Bot admin setup")
                logger.info(f"✅ Created and assigned admin role in {guild.name}")
                return True
                
            except discord.Forbidden:
                logger.warning(f"❌ Cannot create admin role in {guild.name} - no permissions")
                return False
            except Exception as e:
                logger.error(f"❌ Error creating admin role in {guild.name}: {e}")
                return False
        else:
            # Try to assign existing role
            try:
                await guild.me.add_roles(existing_role, reason="Bot admin setup")
                logger.info(f"✅ Assigned existing admin role in {guild.name}")
                return True
            except discord.Forbidden:
                logger.warning(f"❌ Cannot assign admin role in {guild.name} - no permissions")
                return False
    
    except Exception as e:
        logger.error(f"❌ Error setting up permissions in {guild.name}: {e}")
        return False

async def send_welcome_message(guild):
    """Send a welcome message when bot joins a server"""
    try:
        # Try to DM the owner first
        if guild.owner:
            try:
                welcome_embed = create_embed(
                    f"👋 Thanks for adding {bot.user.name}!",
                    f"**Server:** {guild.name}\n\n"
                    f"🚀 **Quick Setup:**\n"
                    f"• Use `/setup_server` for complete server setup\n"
                    f"• Use `/setup_logs` to configure private logging\n"
                    f"• Use `/ticket` to setup the ticket system\n\n"
                    f"💡 **Need Help?**\n"
                    f"Most commands require Administrator permissions. If the bot seems limited, please:\n"
                    f"1. Give the bot Administrator role\n"
                    f"2. Or assign the auto-created '{bot.user.name} Admin' role\n\n"
                    f"📚 Use `/help` to see all available commands!",
                    CONFIG['SUCCESS_COLOR'],
                    thumbnail=bot.user.display_avatar.url
                )
                await guild.owner.send(embed=welcome_embed)
                return
            except discord.Forbidden:
                pass
        
        # If DM fails, try to send in the first available text channel
        for channel in guild.text_channels:
            if channel.permissions_for(guild.me).send_messages:
                welcome_embed = create_embed(
                    f"👋 {bot.user.name} has joined {guild.name}!",
                    f"Thanks for adding me! Use `/setup_server` to get started.\n\n"
                    f"**Important:** For full functionality, please give me Administrator permissions or use the auto-created admin role.",
                    CONFIG['SUCCESS_COLOR']
                )
                await channel.send(embed=welcome_embed)
                break
                
    except Exception as e:
        logger.error(f"Error sending welcome message to {guild.name}: {e}")

async def ensure_bot_permissions(guild, required_permissions=None):
    """Ensure bot has required permissions, try to fix if not"""
    if required_permissions is None:
        required_permissions = ['manage_channels', 'manage_roles', 'manage_messages']
    
    bot_permissions = guild.me.guild_permissions
    
    # Check if bot is admin (bypasses all other checks)
    if bot_permissions.administrator:
        return True
    
    # Check specific permissions
    missing_permissions = []
    for perm in required_permissions:
        if not getattr(bot_permissions, perm, False):
            missing_permissions.append(perm)
    
    if missing_permissions:
        logger.warning(f"Missing permissions in {guild.name}: {missing_permissions}")
        # Try to fix by assigning admin role
        await setup_bot_permissions(guild)
        return False
    
    return True

def require_permissions(**permissions):
    """Decorator to check bot permissions before running commands"""
    def decorator(func):
        async def wrapper(interaction: discord.Interaction, *args, **kwargs):
            # Check if bot has required permissions
            bot_perms = interaction.guild.me.guild_permissions
            missing_perms = []
            
            for perm, required in permissions.items():
                if required and not getattr(bot_perms, perm, False):
                    missing_perms.append(perm)
            
            if missing_perms and not bot_perms.administrator:
                embed = create_embed(
                    "❌ Insufficient Bot Permissions",
                    f"I'm missing these permissions: `{'`, `'.join(missing_perms)}`\n\n"
                    f"**Please give me:**\n"
                    f"• Administrator role (recommended), or\n"
                    f"• The missing permissions listed above\n\n"
                    f"Then try the command again.",
                    CONFIG['ERROR_COLOR']
                )
                await interaction.response.send_message(embed=embed, ephemeral=True)
                return
            
            return await func(interaction, *args, **kwargs)
        return wrapper
    return decorator

# Command to view invoice statistics dashboard
@bot.tree.command(name="invoice_dashboard", description="View detailed invoice statistics")
@app_commands.checks.has_permissions(administrator=True)
async def invoice_dashboard(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    
    try:
        guild_id = str(interaction.guild.id)
        stats = await calculate_invoice_stats(guild_id)
        branding = get_branding_data()  # Use the safe function
        
        # Create the dashboard embed
        embed = create_embed(
            f" {branding['company_name']} Sales Dashboard",
            f"Comprehensive overview of your sales and invoice data",
            branding['primary_color'],
            thumbnail=branding['logo_url'],
            fields=[
                ("📈 Total Invoices", f"{stats['total_invoices']:,}", True),
                ("💰 Total Revenue", f"${stats['total_revenue']:,.2f}", True),
                ("💸 Average Value", f"${stats['avg_invoice_value']:,.2f}", True),
                ("📅 This Month", f"{stats['invoices_this_month']} invoices", True),
                ("💵 Month Revenue", f"${stats['revenue_this_month']:,.2f}", True),
                ("⏰ Last Updated", f"<t:{int(datetime.now().timestamp())}:R>", True)
            ]
        )
        
        # Add top products section
        if stats['top_products']:
            product_text = ""
            for i, product in enumerate(stats['top_products']):
                product_text += f"{i+1}. **{product['name']}** - ${product['revenue']:,.2f} ({product['count']} sales)\n"
            embed.add_field(name="🏆 Top Products", value=product_text, inline=False)
        
        # Add top customers section
        if stats['top_customers']:
            customer_text = ""
            for i, customer in enumerate(stats['top_customers']):
                customer_text += f"{i+1}. **{customer['tag']}** - ${customer['revenue']:,.2f} ({customer['count']} purchases)\n"
            embed.add_field(name="👑 Top Customers", value=customer_text, inline=False)
        
        class InvoiceDashboardView(discord.ui.View):
            def __init__(self):
                super().__init__(timeout=60)
            
            @discord.ui.button(label="📊 Export Data", style=discord.ButtonStyle.primary)
            async def export_data(self, button_interaction: discord.Interaction, button: discord.ui.Button):
                try:
                    # Generate an invoice report as a text file
                    report = f"INVOICE REPORT FOR {interaction.guild.name}\n"
                    report += f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
                    report += f"Total Invoices: {stats['total_invoices']}\n"
                    report += f"Total Revenue: ${stats['total_revenue']:.2f}\n"
                    report += f"Average Invoice Value: ${stats['avg_invoice_value']:.2f}\n\n"
                    
                    report += "TOP PRODUCTS:\n"
                    for i, product in enumerate(stats['top_products']):
                        report += f"{i+1}. {product['name']} - ${product['revenue']:.2f} ({product['count']} sales)\n"
                    
                    report += "\nTOP CUSTOMERS:\n"
                    for i, customer in enumerate(stats['top_customers']):
                        report += f"{i+1}. {customer['tag']} - ${customer['revenue']:.2f} ({customer['count']} purchases)\n"
                    
                    # Convert to file and send
                    file = discord.File(
                        io.BytesIO(report.encode('utf-8')),
                        filename=f"invoice_report_{datetime.now().strftime('%Y%m%d')}.txt"
                    )
                    await button_interaction.response.send_message("Here's your invoice report:", file=file, ephemeral=True)
                except Exception as e:
                    logger.error(f"Error exporting invoice data: {e}")
                    await button_interaction.response.send_message("❌ Error generating report. Please try again.", ephemeral=True)
        
        await interaction.followup.send(embed=embed, view=InvoiceDashboardView(), ephemeral=True)
        
    except Exception as e:
        logger.error(f"Error in invoice_dashboard: {e}")
        await interaction.followup.send("❌ An error occurred while loading the dashboard. Please try again.", ephemeral=True)

# --- ENHANCED MODALS ---
class DMTemplateModal(discord.ui.Modal, title="Create DM Template"):
    template_name = discord.ui.TextInput(
        label="Template Name",
        placeholder="e.g., Netflix Premium Key",
        required=True
    )
    
    embed_title = discord.ui.TextInput(
        label="Embed Title", 
        placeholder="e.g., 🔑 Your {product} Key",
        required=True
    )
    
    embed_description = discord.ui.TextInput(
        label="Embed Description",
        placeholder="Thank you for purchasing {product}! Your key is ready.",
        style=discord.TextStyle.paragraph,
        required=True
    )
    
    custom_message = discord.ui.TextInput(
        label="Additional Message (Optional)",
        placeholder="Enjoy your purchase! Contact support if you need help.",
        style=discord.TextStyle.paragraph,
        required=False
    )
    
    banner_url = discord.ui.TextInput(
        label="Banner Image URL (Optional)",
        placeholder="https://example.com/banner.png",
        required=False
    )

    async def on_submit(self, interaction: discord.Interaction):
        guild_id = str(interaction.guild.id)
        if guild_id not in data_manager.data['dm_templates']:
            data_manager.data['dm_templates'][guild_id] = {}
        
        data_manager.data['dm_templates'][guild_id][self.template_name.value] = {
            'embed_title': self.embed_title.value,
            'embed_description': self.embed_description.value,
            'custom_message': self.custom_message.value or "",
            'banner_url': self.banner_url.value or "",
            'created_by': interaction.user.id,
            'created_at': datetime.now().isoformat()
        }
        
        data_manager.save_category_data('dm_templates')
        await interaction.response.send_message(f"✅ DM template `{self.template_name.value}` created successfully!", ephemeral=True)

class TicketTranscriptModal(discord.ui.Modal, title="Close Ticket & Transcript Options"):
    def __init__(self, ticket_channel):
        super().__init__()
        self.ticket_channel = ticket_channel
        
    reason = discord.ui.TextInput(
        label="Reason for closing",
        placeholder="Issue resolved, customer satisfied, etc.",
        style=discord.TextStyle.paragraph,
        required=True
    )
    
    send_to_user = discord.ui.TextInput(
        label="Send transcript to user? (yes/no)",
        placeholder="yes",
        default="yes",
        max_length=3,
        required=False
    )
    
    send_to_logs = discord.ui.TextInput(
        label="Send to transcript logs? (yes/no)",
        placeholder="yes", 
        default="yes",
        max_length=3,
        required=False
    )

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer()
        
        # Extract ticket creator from channel topic
        creator_id = None
        if self.ticket_channel.topic:
            try:
                creator_id = int(self.ticket_channel.topic.split("Creator ID: ")[1].split(" |")[0])
            except (ValueError, IndexError):
                pass
        
        # Generate transcript
        transcript_text = f"═══════════════════════════════════════════════════\n"
        transcript_text += f"🎫 TICKET TRANSCRIPT\n"
        transcript_text += f"═══════════════════════════════════════════════════\n"
        transcript_text += f"Channel: {self.ticket_channel.name}\n"
        transcript_text += f"Closed by: {interaction.user} ({interaction.user.id})\n"
        transcript_text += f"Reason: {self.reason.value}\n"
        transcript_text += f"Closed at: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}\n"
        transcript_text += f"═══════════════════════════════════════════════════\n\n"
        
        # Get message history
        async for message in self.ticket_channel.history(limit=None, oldest_first=True):
            timestamp = message.created_at.strftime('%Y-%m-%d %H:%M:%S')
            content = message.content or "[No text content]"
            
            if message.embeds:
                content += f"\n[EMBED: {message.embeds[0].title or 'Untitled'}]"
            if message.attachments:
                content += f"\n[ATTACHMENTS: {', '.join([att.filename for att in message.attachments])}]"
                
            transcript_text += f"[{timestamp}] {message.author}: {content}\n"
        
        transcript_file = io.BytesIO(transcript_text.encode())
        
        # Send to user if requested
        if self.send_to_user.value.lower().startswith('y') and creator_id:
            try:
                user = await bot.fetch_user(creator_id)
                if user:
                    embed = create_embed(
                        "🎫 Ticket Transcript",
                        f"Your ticket `{self.ticket_channel.name}` has been closed.\n\n**Reason:** {self.reason.value}",
                        CONFIG['MAIN_COLOR']
                    )
                    transcript_file.seek(0)
                    await user.send(embed=embed, 
                                    file=discord.File(transcript_file, 
                                                    filename=f"transcript-{self.ticket_channel.name}.txt"))
            except discord.Forbidden:
                logger.warning(f"Could not DM transcript to user {creator_id}")
            except Exception as e:
                logger.error(f"Error sending transcript to user: {e}")
        
        # Send to transcript logs if requested
        if self.send_to_logs.value.lower().startswith('y'):
            try:
                log_channel = await find_or_create_channel(interaction.guild, CHANNELS['TRANSCRIPT'])
                if log_channel:
                    embed = create_embed(
                        "🎫 Ticket Closed",
                        f"**Ticket:** `{self.ticket_channel.name}`\n**Closed by:** {interaction.user.mention}\n**Reason:** {self.reason.value}",
                        CONFIG['MAIN_COLOR']
                    )
                    transcript_file.seek(0)
                    await log_channel.send(embed=embed,
                                            file=discord.File(transcript_file, 
                                                            filename=f"transcript-{self.ticket_channel.name}.txt"))
            except Exception as e:
                logger.error(f"Error sending transcript to logs: {e}")
        
        # Log to ticket logs
        await log_to_channel(
            interaction.guild,
            f"🎫 Ticket `{self.ticket_channel.name}` closed by {interaction.user} | Reason: {self.reason.value}",
            CHANNELS['LOG']
        )
        
        await interaction.followup.send("✅ Transcript generated and sent. Deleting channel...", ephemeral=True)
        await self.ticket_channel.delete(reason=f"Ticket closed: {self.reason.value}")

# Modal for creating invoice templates
class InvoiceTemplateModal(discord.ui.Modal, title="Create Invoice Template"):
    template_name = discord.ui.TextInput(
        label="Template Name",
        placeholder="e.g., Premium Invoice",
        required=True
    )
    
    embed_title = discord.ui.TextInput(
        label="Invoice Title", 
        placeholder="e.g., 📄 {product} Invoice #{invoice_id}",
        required=True
    )
    
    embed_description = discord.ui.TextInput(
        label="Invoice Description",
        placeholder="Thank you for purchasing {product}!",
        style=discord.TextStyle.paragraph,
        required=True
    )
    
    custom_fields = discord.ui.TextInput(
        label="Additional Fields (name:value format)",
        placeholder="Support:support@example.com|Warranty:30 Days",
        style=discord.TextStyle.paragraph,
        required=False
    )
    
    color_code = discord.ui.TextInput(
        label="Color Code (hex format, optional)",
        placeholder="#00CED1 (leave empty for default)",
        required=False
    )

    async def on_submit(self, interaction: discord.Interaction):
        guild_id = str(interaction.guild.id)
        if guild_id not in data_manager.data['invoice_templates']:
            data_manager.data['invoice_templates'][guild_id] = {}
        
        # Parse custom fields
        custom_field_dict = {}
        if self.custom_fields.value:
            for field in self.custom_fields.value.split('|'):
                if ':' in field:
                    key, value = field.split(':', 1)
                    custom_field_dict[key.strip()] = value.strip()
        
        # Parse color code
        color = CONFIG['MAIN_COLOR']
        if self.color_code.value:
            try:
                color = int(self.color_code.value.replace('#', ''), 16)
            except ValueError:
                pass
        
        data_manager.data['invoice_templates'][guild_id][self.template_name.value] = {
            'title': self.embed_title.value,
            'description': self.embed_description.value,
            'custom_fields': custom_field_dict,
            'color': color,
            'created_by': interaction.user.id,
            'created_at': datetime.now().isoformat()
        }
        
        data_manager.save_category_data('invoice_templates')
        
        # Show preview
        title = self.embed_title.value.replace("{invoice_id}", "12345").replace("{product}", "Sample Product")
        description = self.embed_description.value.replace("{product}", "Sample Product").replace("{customer}", interaction.user.mention)
        
        amount = 99.99
        fields = [
    ("📅 Date", f"<t:{int(datetime.now().timestamp())}:F>", True),
    ("🆔 Customer ID", f"`{interaction.user.id}`", True),
    ("💰 Amount", f"${amount:.2f}", True)
    ]
        
        for field_name, field_value in custom_field_dict.items():
            fields.append((field_name, field_value, True))
        
        preview_embed = create_embed(
            title,
            description,
            color,
            fields=fields,
            thumbnail=get_branding_data()['logo_url'],  # CHANGED: Circular logo
            image=get_branding_data()['banner_url'],    # Keep banner as image
            footer=get_branding_data()['footer_text']
        )
        
        await interaction.response.send_message(
            content=f"✅ Invoice template `{self.template_name.value}` created successfully! Preview:",
            embed=preview_embed,
            ephemeral=True
        )

# Command to create invoice templates
@bot.tree.command(name="create_invoice_template", description="Create a custom invoice template")
@app_commands.checks.has_permissions(administrator=True)
async def create_invoice_template(interaction: discord.Interaction):
    await interaction.response.send_modal(InvoiceTemplateModal())

# Command to list available invoice templates
@bot.tree.command(name="list_invoice_templates", description="List all available invoice templates")
@app_commands.checks.has_permissions(administrator=True)
async def list_invoice_templates(interaction: discord.Interaction):
    guild_id = str(interaction.guild.id)
    
    if guild_id not in data_manager.data['invoice_templates'] or not data_manager.data['invoice_templates'][guild_id]:
        await interaction.response.send_message("❌ No invoice templates found. Create one with `/create_invoice_template`", ephemeral=True)
        return
    
    templates = data_manager.data['invoice_templates'][guild_id]
    
    embed = create_embed(
        "📄 Available Invoice Templates",
        f"Your server has {len(templates)} invoice templates.",
        CONFIG['MAIN_COLOR'],
        thumbnail=data_manager.data['branding']['logo_url']
    )
    
    for name, template in templates.items():
        created_at = datetime.fromisoformat(template['created_at'])
        embed.add_field(
            name=name,
            value=f"**Title:** {template['title'][:30]}...\n**Created:** <t:{int(created_at.timestamp())}:R>",
            inline=True
        )
    
    await interaction.response.send_message(embed=embed, ephemeral=True)        

class EnhancedVouchModal(discord.ui.Modal):  # ✅ Correct class definition
    def __init__(self, prefilled_product=None):  # ✅ Define __init__ method
        super().__init__(title="Leave a Product Review")  # ✅ Pass title here
        
        # Create and add items inside __init__ method
        self.product_input = discord.ui.TextInput(
            label="Product Name",
            placeholder="e.g., Netflix Premium Account",
            default=prefilled_product or "",
            required=True
        )
        self.add_item(self.product_input)
        
        self.rating_input = discord.ui.TextInput(
            label="Star Rating (1-5)",
            placeholder="5",
            max_length=1,
            required=True
        )
        self.add_item(self.rating_input)
        
        self.experience_input = discord.ui.TextInput(
            label="Your Experience",
            placeholder="Fast delivery, working perfectly, great seller!",
            style=discord.TextStyle.paragraph,
            max_length=500,
            required=True
        )
        self.add_item(self.experience_input)
        
        self.supporter_input = discord.ui.TextInput(
            label="Staff Member Who Helped (Optional)",
            placeholder="e.g., @StaffMember",
            required=False
        )
        self.add_item(self.supporter_input)

    async def on_submit(self, interaction: discord.Interaction):
        try:
            rating = int(self.rating_input.value)
            if not 1 <= rating <= 5:
                await interaction.response.send_message("⭐ Rating must be between 1 and 5.", ephemeral=True)
                return
        except ValueError:
            await interaction.response.send_message("❌ Invalid rating. Please enter a number between 1 and 5.", ephemeral=True)
            return

        # Store vouch data
        user_id = str(interaction.user.id)
        if user_id not in data_manager.data['vouches']:
            data_manager.data['vouches'][user_id] = {"count": 0, "vouches": []}
        
        data_manager.data['vouches'][user_id]["count"] += 1
        data_manager.data['vouches'][user_id]["vouches"].append({
            "product": self.product_input.value,
            "rating": rating,
            "experience": self.experience_input.value,
            "supporter": self.supporter_input.value,
            "timestamp": datetime.now().isoformat()
        })
        data_manager.save_category_data('vouches')

        # Create vouch embed
        stars = "⭐" * rating
        vouch_embed = create_embed(
            f"🏆 Customer Review by {interaction.user.display_name}",
            f"**Product:** `{self.product_input.value}`\n**Rating:** {stars} ({rating}/5)",
            CONFIG['SUCCESS_COLOR'],
            thumbnail=interaction.user.display_avatar.url,
            fields=[
                ("💭 Experience", f"```{self.experience_input.value}```", False),
                ("📊 Total Reviews", f"{data_manager.data['vouches'][user_id]['count']}", True),
                ("📅 Date", f"<t:{int(datetime.now().timestamp())}:R>", True)
            ]
        )
        
        if self.supporter_input.value:
            vouch_embed.add_field(name="👨‍💼 Supported By", value=self.supporter_input.value, inline=True)

        # Send to your specified vouch channel
        target_channel = interaction.guild.get_channel(TARGET_VOUCH_CHANNEL_ID)
        if target_channel:
            await target_channel.send(embed=vouch_embed)
            await interaction.response.send_message("✅ Thank you for your review! It has been posted to the vouch channel.", ephemeral=True)
        else:
    # Fallback to log channel if target channel not found
            vouch_channel = await find_or_create_channel(interaction.guild, CHANNELS['VOUCH'])
            if vouch_channel:
                await vouch_channel.send(embed=vouch_embed)
            await interaction.response.send_message("✅ Vouch submitted successfully!", ephemeral=True)
        
# --- ENHANCED TICKET SYSTEM ---
class AdvancedTicketView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)
    
    @discord.ui.button(label="Close & Transcript", style=discord.ButtonStyle.red, emoji="📋", custom_id="close_transcript")
    async def close_transcript(self, interaction: discord.Interaction, button: discord.ui.Button):
        # Check permissions
        is_creator = False
        if interaction.channel.topic and "Creator ID:" in interaction.channel.topic:
            try:
                creator_id = interaction.channel.topic.split("Creator ID: ")[1].split(" |")[0]
                if creator_id == str(interaction.user.id):
                    is_creator = True
            except (IndexError, ValueError):
                pass

        if not (interaction.user.guild_permissions.manage_channels or is_creator):
            await interaction.response.send_message("❌ You can only close your own ticket or need 'Manage Channels' permission.", ephemeral=True)
            return
        
        await interaction.response.send_modal(TicketTranscriptModal(interaction.channel))
    
    @discord.ui.button(label="Delete Ticket", style=discord.ButtonStyle.danger, emoji="🗑️", custom_id="delete_ticket")
    async def delete_ticket(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not interaction.user.guild_permissions.manage_channels:
            await interaction.response.send_message("❌ You need 'Manage Channels' permission to delete tickets.", ephemeral=True)
            return
        
        class ConfirmDeleteView(discord.ui.View):
            def __init__(self):
                super().__init__(timeout=30)
            
            @discord.ui.button(label="Confirm Delete", style=discord.ButtonStyle.danger, emoji="⚠️")
            async def confirm_delete(self, confirm_interaction: discord.Interaction, confirm_button: discord.ui.Button):
                await log_to_channel(
                    interaction.guild,
                    f"🗑️ Ticket `{interaction.channel.name}` DELETED by {confirm_interaction.user} (NO TRANSCRIPT)",
                    CHANNELS['LOG']
                )
                await confirm_interaction.response.send_message("🗑️ Deleting ticket...", ephemeral=True)
                await interaction.channel.delete(reason=f"Ticket deleted by {confirm_interaction.user}")
            
            @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
            async def cancel_delete(self, cancel_interaction: discord.Interaction, cancel_button: discord.ui.Button):
                await cancel_interaction.message.delete()
                await cancel_interaction.response.send_message("❌ Deletion cancelled.", ephemeral=True)
        
        embed = create_embed(
            "⚠️ Delete Ticket",
            f"**WARNING:** This will immediately delete the ticket `{interaction.channel.name}` with **NO TRANSCRIPT**.\n\nThis action cannot be undone.",
            CONFIG['ERROR_COLOR']
        )
        await interaction.response.send_message(embed=embed, view=ConfirmDeleteView(), ephemeral=True)

class EnhancedTicketDropdown(discord.ui.Select):
    def __init__(self):
        options = [
            discord.SelectOption(label="Purchase", emoji="💳", description="Buy products, services, or accounts", value="purchase"),
            discord.SelectOption(label="Exchange", emoji="🔄", description="Money Exchange", value="exchange"),
            discord.SelectOption(label="General Support", emoji="💬", description="Technical help and general questions", value="support"),
            discord.SelectOption(label="Reseller Application", emoji="🤝", description="Apply to become an authorized reseller", value="reseller"),
            discord.SelectOption(label="Media Partner", emoji="🖼️", description="Media partner, content creation requests", value="media"),
            discord.SelectOption(label="Giveaway", emoji="🎁", description="Select if you have won a giveaway.", value="giveaway"),
            discord.SelectOption(label="Report Issues", emoji="🛡️", description="Report bugs, security issues, or server members", value="report"),
        ]
        super().__init__(
            placeholder="🎫 Choose your support category...",
            min_values=1,
            max_values=1,
            options=options,
            custom_id="enhanced_ticket_dropdown"
        )

    async def callback(self, interaction: discord.Interaction):
        selected = self.values[0]

        # Create a fresh view to reset the dropdown appearance
        fresh_view = EnhancedTicketView()
        
        # Update the message with the fresh view (this resets the visual state)
        try:
            await interaction.response.edit_message(view=fresh_view)
        except discord.InteractionResponse:
            # If response was already used, use followup
            await interaction.edit_original_response(view=fresh_view)
        
        # Now send the ephemeral response
        try:
            await interaction.followup.send("Processing your request...", ephemeral=True, delete_after=1)
        except:
            pass

        # Block duplicates only for the SAME category (not any ticket)
        existing = None
        for channel in interaction.guild.text_channels:
            if (
                channel.topic
                and f"Creator ID: {interaction.user.id}" in channel.topic
                and f"Reason: {selected.title()}" in channel.topic
            ):
                existing = channel
                break

        if existing:
            try:
                await interaction.followup.send(
                    f"⚠️ You already have an open ticket in **{selected.title()}**: {existing.mention}\n"
                    f"Close it before creating another one in this category.",
                    ephemeral=True
                )
            except:
                pass
            return

        # Create ticket
        try:
            new_channel = await create_ticket_channel(interaction, selected)

            if new_channel:
                try:
                    await interaction.followup.send(
                        f"✅ **Ticket Created!** Your {selected} ticket is ready: {new_channel.mention}",
                        ephemeral=True
                    )
                except:
                    pass
            else:
                try:
                    await interaction.followup.send(
                        "❌ Failed to create the ticket. I might be missing permissions.",
                        ephemeral=True
                    )
                except:
                    pass

        except Exception as e:
            logger.error(f"Error during ticket creation callback: {e}")
            try:
                await interaction.followup.send(
                    "❌ An unexpected error occurred while creating the ticket.",
                    ephemeral=True
                )
            except:
                pass

async def create_ticket_channel(
    interaction: discord.Interaction,
    category_key: str,
    product_hint: str = None
) -> Optional[discord.TextChannel]:
    guild = interaction.guild

    # PREVENT DUPLICATES ONLY FOR THIS CATEGORY (not any ticket)
    for channel in guild.text_channels:
        if (
            channel.topic
            and f"Creator ID: {interaction.user.id}" in channel.topic
            and f"Reason: {category_key.title()}" in channel.topic
        ):
            return channel  # Already open for this category

    # Ensure ticket category exists
    category = discord.utils.get(guild.categories, name=CATEGORIES['TICKETS'])
    if not category:
        try:
            category = await guild.create_category(CATEGORIES['TICKETS'])
        except discord.Forbidden:
            return None

    # Build overwrites (+ support role if configured)
    overwrites = {
        guild.default_role: discord.PermissionOverwrite(view_channel=False),
        interaction.user: discord.PermissionOverwrite(view_channel=True, send_messages=True, read_message_history=True),
        guild.me: discord.PermissionOverwrite(view_channel=True, send_messages=True, manage_messages=True, read_message_history=True)
    }

    guild_id = str(guild.id)
    support_role = None
    if guild_id in data_manager.data['ticket_config'] and 'support_role_id' in data_manager.data['ticket_config'][guild_id]:
        support_role = guild.get_role(data_manager.data['ticket_config'][guild_id]['support_role_id'])
        if support_role:
            overwrites[support_role] = discord.PermissionOverwrite(view_channel=True, send_messages=True, read_message_history=True)

    category_info = {
        "purchase": {"emoji": "💳", "color": CONFIG['SUCCESS_COLOR'], "title": "Purchase Support"},
        "exchange": {"emoji": "🔄", "color": CONFIG['WARNING_COLOR'], "title": "Exchange & Refunds"},
        "support": {"emoji": "💬", "color": CONFIG['MAIN_COLOR'], "title": "General Support"},
        "reseller": {"emoji": "🤝", "color": CONFIG['MAIN_COLOR'], "title": "Reseller Application"},
        "media": {"emoji": "🎬", "color": CONFIG['MAIN_COLOR'], "title": "Media Support"},
        "giveaway": {"emoji": "🎁", "color": CONFIG['SUCCESS_COLOR'], "title": "Giveaway Support"},
        "report": {"emoji": "🛡️", "color": CONFIG['ERROR_COLOR'], "title": "Issue Report"}
    }
    info = category_info.get(category_key, {"emoji": "🎫", "color": CONFIG['MAIN_COLOR'], "title": "Support Ticket"})

    # Create ticket channel
    user_name_safe = re.sub(r'[^a-zA-Z0-9]', '', interaction.user.name.lower())
    channel_name = f"ticket-{user_name_safe[:10]}-{random.randint(100,999)}"

    topic_extra = f" | Product: {product_hint}" if product_hint else ""
    try:
        channel = await guild.create_text_channel(
            name=channel_name,
            category=category,
            overwrites=overwrites,
            topic=f"Creator ID: {interaction.user.id} | Reason: {category_key.title()}{topic_extra}"
        )
    except discord.Forbidden:
        return None

    details_lines = [
        f"**Ticket created by:** {interaction.user.mention}",
        f"**Category:** {category_key.title()}",
    ]
    if product_hint:
        details_lines.append(f"**Product:** {product_hint}")

    details_lines += [
        "",
        "📞 A staff member will be with you shortly!",
        "",
        "**To speed things up, please provide:**",
        "• A brief description of your request",
        "• Screenshots if applicable",
        "• Order details (if purchase related)",
    ]
    desc = "\n".join(details_lines)

    ticket_embed = create_embed(
        f"{info['emoji']} {info['title']}",
        desc,
        info['color'],
        thumbnail=interaction.user.display_avatar.url,
        fields=[
            ("🕐 Created", f"<t:{int(datetime.now().timestamp())}:R>", True),
            ("👤 User ID", f"`{interaction.user.id}`", True),
            ("📋 Category", category_key.title(), True)
        ],
        footer="Your custom footer text here • Please be patient"
    )

    ping_content = support_role.mention if support_role else ""
    await channel.send(
        content=f"{interaction.user.mention} {ping_content}".strip(),
        embed=ticket_embed,
        view=AdvancedTicketView()
    )

    await log_to_channel(
        guild,
        f"🎫 New {category_key} ticket created by {interaction.user} (`{interaction.user.id}`) in {channel.mention}",
        CHANNELS['LOG']
    )

    return channel

class EnhancedTicketView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)
        self.add_item(EnhancedTicketDropdown())


# --- PRODUCT AND PURCHASE VIEWS ---

class CustomerVouchView(discord.ui.View):
    def __init__(self, user=None, product=None):
        super().__init__(timeout=None)
        self.user = user
        self.product = product

    @discord.ui.button(label="Leave a Vouch", style=discord.ButtonStyle.green, emoji="⭐", custom_id="customer_vouch_btn")
    async def leave_vouch(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self.user and interaction.user.id != self.user.id:
            await interaction.response.send_message("❌ This vouch request is for another user.", ephemeral=True)
            return
        
        try:
            modal = EnhancedVouchModal(self.product)
            await interaction.response.send_modal(modal)
        except Exception as e:
            logger.error(f"Error sending vouch modal: {e}")
            await interaction.response.send_message("❌ Failed to open vouch form. Please try again.", ephemeral=True)

# --- PRODUCT AND PURCHASE VIEWS ---

class ProductDetailsView(discord.ui.View):
    def __init__(self, ticket_url=None, website_url=None):
        super().__init__(timeout=None)
        
        # Only add ticket button if URL is available
        if ticket_url:
            self.add_item(discord.ui.Button(
                label="Create Ticket",
                style=discord.ButtonStyle.success,
                url=ticket_url,
                emoji="🎫"
            ))
        else:
            # Add a disabled button with explanation
            disabled_button = discord.ui.Button(
                label="Ticket System Not Configured",
                style=discord.ButtonStyle.secondary,
                disabled=True,
                emoji="⚠️"
            )
            self.add_item(disabled_button)
        
        # Add website button
        website_url = website_url or "https://your.website.com"
        self.add_item(discord.ui.Button(
            label="Website",
            style=discord.ButtonStyle.link,
            url=website_url,
            emoji="🔗"
        ))


class ProductPostView(discord.ui.View):
    def __init__(self, template_data: Dict, guild_id: int):
        super().__init__(timeout=None)
        self.template_data = template_data
        self.guild_id = guild_id
        
        self.more_button = discord.ui.Button(
            label="Mehr Infos",
            style=discord.ButtonStyle.primary,
            emoji="🔍"
        )
        self.more_button.callback = self.more_details
        self.add_item(self.more_button)

    async def more_details(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        
        template = self.template_data
        
        # Create detailed embed with proper formatting
        detail_embed = discord.Embed(
            title=f"**{template['title']}**",
            description=f"**{template.get('description', 'Product details below')}**",
            color=template.get('color', 0x2F3136)
        )
        
        # Add features with proper formatting
        if template.get('features'):
            detail_embed.add_field(
                name="**✨ Features:**",
                value=template['features'],
                inline=False
            )
        
        # Add pricing with code blocks
        if template.get('price'):
            detail_embed.add_field(
                name="**💰 Pricing:**",
                value=f"```{template['price']}```",
                inline=False
            )
        
        # Add image if exists
        if template.get('image_url'):
            detail_embed.set_image(url=template['image_url'])
        
        # Create buttons for Ticket and Website
        view = discord.ui.View()
        
        # Find ticket panel URL
        guild_id_str = str(self.guild_id)
        ticket_config = data_manager.data['ticket_config'].get(guild_id_str, {})
        ticket_url = None
        
        if 'ticket_channel_id' in ticket_config and 'ticket_message_id' in ticket_config:
            channel_id = ticket_config['ticket_channel_id']
            message_id = ticket_config['ticket_message_id']
            ticket_url = f"https://discord.com/channels/{self.guild_id}/{channel_id}/{message_id}"
        
        # Add buttons with proper formatting
        if ticket_url:
            view.add_item(discord.ui.Button(
                label="🎫 Create Ticket",
                style=discord.ButtonStyle.link,
                url=ticket_url,
                emoji="🎫"
            ))
        
        view.add_item(discord.ui.Button(
            label="🌐 Website",
            style=discord.ButtonStyle.link,
            url="https://your-website.com",
            emoji="🔗"
        ))
        
        await interaction.followup.send(embed=detail_embed, view=view, ephemeral=True)


class EnhancedProductDetailsView(discord.ui.View):
    def __init__(self, ticket_url=None, guild=None):
        super().__init__(timeout=None)
        self.guild = guild
        
        if ticket_url:
            # Add working ticket button
            self.add_item(discord.ui.Button(
                label="Create Ticket",
                style=discord.ButtonStyle.success,
                url=ticket_url,
                emoji="🎫"
            ))
        else:
            # Add fallback create ticket button
            self.add_item(CreateTicketDirectButton())
        
        # Add website button
        website_url = "https://your.website.com"
        self.add_item(discord.ui.Button(
            label="Website",
            style=discord.ButtonStyle.link,
            url=website_url,
            emoji="🔗"
        ))


class CreateTicketDirectButton(discord.ui.Button):
    def __init__(self):
        super().__init__(
            label="Create Ticket Here",
            style=discord.ButtonStyle.primary,
            emoji="🎫"
        )
    
    async def callback(self, interaction: discord.Interaction):
        # Create ticket directly instead of redirecting
        try:
            # Create a purchase ticket
            ticket_channel = await create_ticket_channel(
                interaction=interaction,
                category_key="purchase",
                product_hint="Product inquiry from embed"
            )
            
            if ticket_channel:
                await interaction.response.send_message(
                    f"✅ **Ticket Created!** Your support ticket is ready: {ticket_channel.mention}",
                    ephemeral=True
                )
            else:
                await interaction.response.send_message(
                    "❌ Failed to create ticket. Please contact an administrator.",
                    ephemeral=True
                )
                
        except Exception as e:
            logger.error(f"Error creating direct ticket: {e}")
            await interaction.response.send_message(
                "❌ An error occurred. Please try using the main ticket system.",
                ephemeral=True
            )

# --- KEY DELIVERY SYSTEM ---
class EnhancedDeliverKeyView(discord.ui.View):
    def __init__(self, user, product, amount_spent=0.0):
        super().__init__(timeout=300)
        self.user = user
        self.product = product
        self.amount_spent = amount_spent

    @discord.ui.button(label="Deliver Key", style=discord.ButtonStyle.success, emoji="🔑")
    async def deliver_key(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not interaction.user.guild_permissions.administrator:
            await interaction.response.send_message("❌ Administrator permissions required.", ephemeral=True)
            return

        button.disabled = True
        await interaction.response.edit_message(view=self)
        
        # Check stock
        stock = await key_manager.get_product_stock(self.product)
        if stock <= 0:
            embed = create_embed(
                "❌ No Keys Available", 
                f"No available keys found for **{self.product}**.", 
                CONFIG['ERROR_COLOR']
            )
            await interaction.edit_original_response(embed=embed, view=None)
            return

        # Deliver key
        user_tag = f"{self.user.name}#{self.user.discriminator}"
        key = await key_manager.use_product_key(self.product, user_tag, self.user.id, self.amount_spent)
        
        if not key:
            embed = create_embed(
                "❌ Delivery Failed", 
                f"Failed to retrieve key for **{self.product}**.", 
                CONFIG['ERROR_COLOR']
            )
            await interaction.edit_original_response(embed=embed, view=None)
            return

        # Get user's purchase data for tier calculation
        purchase_data = await key_manager.get_user_purchases_detailed(self.user.id)
        tier_name, tier_color = get_customer_tier(purchase_data['lifetime_spent'])

        # Get DM template
        guild_id = str(interaction.guild.id)
        template_data = data_manager.data['dm_templates'].get(guild_id, {}).get('default', {
            'embed_title': '🔑 Your {product} Key',
            'embed_description': 'Thank you for your purchase of **{product}**!\n\nYour license key is provided below. Please save it immediately.',
            'custom_message': '',
            'banner_url': ''
        })
        
        # Create DM embed
        dm_embed = create_embed(
            template_data['embed_title'].format(product=self.product),
            template_data['embed_description'].format(product=self.product),
            CONFIG['SUCCESS_COLOR'],
            fields=[
                ("🔐 License Key", f"```{key}```", False),
                ("📦 Product", self.product, True),
                ("⏰ Delivered", f"<t:{int(datetime.now().timestamp())}:F>", True),
                ("🏆 Customer Tier", tier_name, True)
            ],
            image=template_data.get('banner_url') or None
        )
        dm_embed.set_footer(text=f"{interaction.guild.name} • Keep this key safe!")
        
        # Try to send DM
        dm_success = False
        try:
            dm_content = template_data.get('custom_message', '').format(product=self.product) if template_data.get('custom_message') else None
            await self.user.send(content=dm_content, embed=dm_embed)
            dm_success = True
        except discord.Forbidden:
            pass

        # Update user roles based on tier
        member = interaction.guild.get_member(self.user.id)
        if member:
            try:
                # Remove old tier roles and add new one
                tier_roles = ["Diamond Customer", "Platinum Customer", "Gold Customer", "Silver Customer", "Bronze Customer", "New Customer"]
                roles_to_remove = []
                for role_name in tier_roles:
                    role = discord.utils.get(interaction.guild.roles, name=role_name)
                    if role and role in member.roles:
                        roles_to_remove.append(role)
                if roles_to_remove:
                    await member.remove_roles(*roles_to_remove, reason="Tier update")
                
                # Add appropriate tier role
                tier_role_name = tier_name.split(" ", 1)[1]  # Remove emoji
                tier_role = discord.utils.get(interaction.guild.roles, name=tier_role_name)
                if tier_role:
                    await member.add_roles(tier_role, reason="Customer tier assignment")
                
            except discord.Forbidden:
                logger.warning(f"Failed to update roles for {member.name}")

        # Create success response
        remaining_stock = await key_manager.get_product_stock(self.product)
        
        success_embed = create_embed(
            "✅ Key Delivered Successfully",
            f"Product key for **{self.product}** has been delivered to {self.user.mention}",
            CONFIG['SUCCESS_COLOR'],
            fields=[
                ("👤 Customer", f"{self.user.mention}\n`{user_tag}`", True),
                ("📦 Product", self.product, True), 
                ("📊 Stock Remaining", f"{remaining_stock} keys", True),
                ("💰 Amount Spent", f"${self.amount_spent:.2f}" if self.amount_spent > 0 else "Not specified", True),
                ("🏆 Customer Tier", tier_name, True),
                ("💬 DM Status", "✅ Delivered" if dm_success else "❌ Failed (DMs closed)", True)
            ]
        )
        
        if not dm_success:
            success_embed.add_field(
                name="⚠️ DM Failed - Key Below",
                value=f"{self.user.mention} Your DMs are disabled!\n\n🔐 **Your Key:** ||`{key}`||",
                inline=False
            )

        await interaction.edit_original_response(embed=success_embed, view=PostPurchaseActionsView(self.user, self.product))

class PostPurchaseActionsView(discord.ui.View):
    def __init__(self, user, product):
        super().__init__(timeout=None)
        self.user = user
        self.product = product

    @discord.ui.button(label="Generate Invoice", style=discord.ButtonStyle.blurple, emoji="📄")
    async def generate_invoice(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not interaction.user.guild_permissions.administrator:
            await interaction.response.send_message("❌ Administrator permissions required.", ephemeral=True)
            return
        
        # Generate invoice number and timestamp
        invoice_num = random.randint(10000, 99999)
        timestamp = int(datetime.now().timestamp())
        
        # Get branding data safely
        branding = get_branding_data()
        
        # Store invoice data
        invoice_data = {
            "invoice_id": invoice_num,
            "product": self.product,
            "customer_id": self.user.id,
            "customer_tag": f"{self.user.name}#{self.user.discriminator}",
            "processor_id": interaction.user.id,
            "processor_tag": f"{interaction.user.name}#{interaction.user.discriminator}",
            "timestamp": timestamp,
            "guild_id": interaction.guild.id,
            "template_used": "post_purchase_action",
            "amount": 0.0  # Default amount for post-purchase invoices
        }
        
        # Save to data storage
        guild_id = str(interaction.guild.id)
        if guild_id not in data_manager.data.get('invoices', {}):
            if 'invoices' not in data_manager.data:
                data_manager.data['invoices'] = {}
            data_manager.data['invoices'][guild_id] = {}
        
        data_manager.data['invoices'][guild_id][str(invoice_num)] = invoice_data
        data_manager.save_category_data('invoices')
        
        # Create invoice embed
        invoice_embed = create_embed(
            f"📄 Purchase Invoice #{invoice_num}",
            f"**Product:** {self.product}\n**Customer:** {self.user.mention}\n**Processed by:** {interaction.user.mention}",
            branding['primary_color'],
            fields=[
                ("📅 Date", f"<t:{timestamp}:F>", True),
                ("🆔 Customer ID", f"`{self.user.id}`", True),
                ("✅ Status", "Completed", True),
                ("📧 Contact", "support@northernhub.com", True)
            ],
            thumbnail=branding['logo_url'],
            footer=branding['footer_text']
        )
        
        # Send invoice to DM
        dm_status = "❌ Could not send to DMs"
        try:
            await self.user.send(embed=invoice_embed)
            dm_status = "✅ Sent to DMs"
        except discord.Forbidden:
            dm_status = "❌ Could not send to DMs (DMs closed)"
        except Exception as e:
            logger.error(f"Error sending invoice to DM: {e}")
            dm_status = "❌ Error sending to DMs"
        
        # Log to invoice channel (NEW FEATURE)
        invoice_logged = await log_invoice_to_channel(interaction.guild, invoice_data, invoice_embed)
        
        # Also log to transcript channel (existing functionality)
        try:
            transcript_channel = await find_or_create_channel(interaction.guild, CHANNELS['TRANSCRIPT'])
            if transcript_channel:
                transcript_embed = create_embed(
                    f"📄 Invoice Generated #{invoice_num}",
                    f"**Product:** {self.product}\n**Customer:** {self.user.mention} (`{self.user.id}`)\n**Processed by:** {interaction.user.mention}",
                    CONFIG['MAIN_COLOR'],
                    fields=[
                        ("📅 Date", f"<t:{timestamp}:F>", True),
                        ("🏷️ Status", "Completed", True),
                        ("💬 DM Status", dm_status, True),
                        ("📋 Invoice Log", "✅ Logged to invoice channel" if invoice_logged else "❌ Failed to log", True)
                    ]
                )
                await transcript_channel.send(embed=transcript_embed)
        except Exception as e:
            logger.error(f"Error saving invoice to transcript channel: {e}")
        
        # Confirm to the admin
        confirmation_embed = create_embed(
            "✅ Invoice Generated Successfully",
            f"Invoice #{invoice_num} for {self.user.mention} has been generated and logged.",
            CONFIG['SUCCESS_COLOR'],
            fields=[
                ("📦 Product", self.product, True),
                ("💬 DM Status", dm_status, True),
                ("📋 Invoice Channel", "✅ Logged" if invoice_logged else "❌ Failed", True),
                ("🔗 Invoice ID", f"#{invoice_num}", True)
            ]
        )
        await interaction.response.send_message(embed=confirmation_embed, ephemeral=True)

    @discord.ui.button(label="Request Vouch", style=discord.ButtonStyle.green, emoji="⭐")
    async def request_vouch(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not interaction.user.guild_permissions.administrator:
            await interaction.response.send_message("❌ Administrator permissions required.", ephemeral=True)
            return
        
        # Create vouch request embed
        vouch_embed = create_embed(
            "⭐ Vouch Request",
            f"**{self.user.mention}** - How was your experience with **{self.product}**?\n\nClick the button below to leave a review!",
            CONFIG['SUCCESS_COLOR']
        )
        
        await interaction.response.send_message(embed=vouch_embed, view=CustomerVouchView(self.user, self.product))

class GiveawayEntryView(discord.ui.View):
    def __init__(self, giveaway_id):
        super().__init__(timeout=None)
        self.giveaway_id = giveaway_id

    @discord.ui.button(label="Enter Giveaway", style=discord.ButtonStyle.success, emoji="🎁")
    async def enter_giveaway(self, interaction: discord.Interaction, button: discord.ui.Button):
        guild_id_str = str(interaction.guild.id)
        
        # Since the view is persistent, we might need to look up the giveaway by message_id
        giveaway_data = data_manager.data['giveaways'].get(guild_id_str, {})
        giveaway = None
        for msg_id, gw_info in giveaway_data.items():
            if gw_info.get("giveaway_id") == self.giveaway_id:
                giveaway = gw_info
                break

        if not giveaway:
            await interaction.response.send_message("❌ This giveaway is no longer active.", ephemeral=True)
            return
        
        if interaction.user.id in giveaway["entries"]:
            await interaction.response.send_message("⚠️ You are already entered in this giveaway!", ephemeral=True)
            return
        
        # Add entry
        giveaway["entries"].append(interaction.user.id)
        data_manager.save_category_data('giveaways')
        
        # Update the button label within the view before sending the edit request
        button.label = f"Enter Giveaway ({len(giveaway['entries'])})"
        
        await interaction.response.edit_message(view=self)
        await interaction.followup.send("🎉 You've successfully entered the giveaway! Good luck!", ephemeral=True)

async def end_giveaway_logic(guild: discord.Guild, giveaway_info: dict):
    """Enhanced giveaway ending with customizable messages"""
    try:
        channel = bot.get_channel(giveaway_info['channel_id'])
        if not channel:
            return
        
        try:
            message = await channel.fetch_message(giveaway_info['message_id'])
        except discord.NotFound:
            return

        entries = giveaway_info['entries']
        prize = giveaway_info['prize']
        winner_count = giveaway_info['winner_count']

        if not entries:
            end_embed = create_embed(
                "🎁 Giveaway Ended - No Entries",
                f"The giveaway for **{prize}** has ended with no participants.",
                CONFIG['WARNING_COLOR'],
                fields=[("📊 Total Entries", "0", True)]
            )
            await message.edit(embed=end_embed, view=None)
            return

        # Select winners
        unique_entries = list(set(entries))
        winners = random.sample(unique_entries, min(winner_count, len(unique_entries)))
        
        winner_mentions = []
        for uid in winners:
            try:
                # Use guild.get_member for mentions, fallback to fetch_user
                user = guild.get_member(uid) or await bot.fetch_user(uid)
                winner_mentions.append(f"🏆 {user.mention}")
            except:
                continue

        if winner_mentions:
            end_embed = create_embed(
                "🎉 GIVEAWAY ENDED! 🎉",
                f"**Prize:** {prize}\n\n**Winners:**\n" + "\n".join(winner_mentions),
                CONFIG['SUCCESS_COLOR'],
                fields=[
                    ("📊 Total Entries", f"{len(entries)}", True),
                    ("🏆 Winners Selected", f"{len(winner_mentions)}", True),
                    ("⏰ Ended", f"<t:{int(datetime.now().timestamp())}:R>", True)
                ]
            )
            
            # Congratulations message
            congrats_msg = f"🎉 **CONGRATULATIONS!** 🎉\n\n{', '.join([w.split(' ', 1)[1] for w in winner_mentions])}\n\nYou have won **{prize}**! 🎁\n\n📞 Please contact a staff member to claim your prize!"
            
            await channel.send(congrats_msg)
            await message.edit(embed=end_embed, view=None)

        # Cleanup
        guild_id_str = str(guild.id)
        giveaway_message_id = str(giveaway_info['message_id'])
        if (guild_id_str in data_manager.data['giveaways'] and 
            giveaway_message_id in data_manager.data['giveaways'][guild_id_str]):
            del data_manager.data['giveaways'][guild_id_str][giveaway_message_id]
            data_manager.save_category_data('giveaways')

    except Exception as e:
        logger.error(f"Error ending giveaway: {e}")

# --- BACKGROUND TASKS ---
temp_bans = {}  # {(guild_id, user_id): end_time}

@tasks.loop(minutes=1)
async def check_temp_bans():
    current_time = datetime.now(timezone.utc)
    to_unban = []
    
    for key, end_time in list(temp_bans.items()):
        guild_id, user_id = key
        if current_time >= end_time:
            guild = bot.get_guild(guild_id)
            if guild:
                try:
                    user = await bot.fetch_user(user_id)
                    await guild.unban(user, reason="Tempban expired")
                    unban_embed = create_embed(
                        "✅ Tempban Expired",
                        f"**User:** {user}\n**ID:** {user.id}",
                        CONFIG['SUCCESS_COLOR'],
                        fields=[("Unbanned", f"<t:{int(current_time.timestamp())}:F>", True)]
                    )
                    await log_to_channel(guild, f"Tempban expired for {user}", CHANNELS['MOD_LOG'], embed=unban_embed)
                    to_unban.append(key)
                except discord.NotFound:
                    to_unban.append(key)
                except discord.Forbidden:
                    logger.warning(f"Missing permissions to unban in guild {guild.name}")
    
    for key in to_unban:
        del temp_bans[key]

@tasks.loop(minutes=1)
async def check_giveaways():
    current_time = datetime.now(timezone.utc)
    
    all_giveaways = data_manager.data['giveaways'].copy()

    for guild_id_str, giveaways in all_giveaways.items():
        guild = bot.get_guild(int(guild_id_str))
        if not guild:
            continue
            
        expired_giveaways = []
        for message_id, giveaway_info in giveaways.items():
            try:
                end_time = datetime.fromisoformat(giveaway_info['end_time'])
                if current_time >= end_time:
                    expired_giveaways.append(giveaway_info)
            except (ValueError, KeyError):
                expired_giveaways.append(giveaway_info)
        
        for giveaway_info in expired_giveaways:
            await end_giveaway_logic(guild, giveaway_info)

@tasks.loop(hours=CONFIG['BACKUP_INTERVAL_HOURS'])
async def backup_data_task():
    """Enhanced backup system"""
    try:
        if not os.path.exists(CONFIG['BACKUP_FOLDER']):
            os.makedirs(CONFIG['BACKUP_FOLDER'])
            
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        
        backup_files = []
        for file in os.listdir(CONFIG['BACKUP_FOLDER']):
            if file.startswith('complete_backup_') and file.endswith('.json'):
                backup_files.append(os.path.join(CONFIG['BACKUP_FOLDER'], file))
                
        backup_files.sort(key=os.path.getctime)
        while len(backup_files) >= CONFIG['MAX_BACKUPS']:
            os.remove(backup_files.pop(0))
            
        backup_data = {
            "timestamp": timestamp,
            "bot_info": {"guild_count": len(bot.guilds), "user_count": len(bot.users)},
            **data_manager.data
        }
        
        backup_path = os.path.join(CONFIG['BACKUP_FOLDER'], f"complete_backup_{timestamp}.json")
        success = data_manager.save_data(backup_path, backup_data)
        
        if success:
            logger.info(f"✅ Data backup complete at {timestamp}")
        else:
            logger.error(f"❌ Backup failed at {timestamp}")
            
    except Exception as e:
        logger.error(f"❌ Failed to perform data backup: {e}")

@tasks.loop(minutes=15)
async def update_stats_channels():
    """Update server statistics channels"""
    for guild in bot.guilds:
        try:
            guild_id = str(guild.id)
            stats_data = data_manager.data['stats_channels'].get(guild_id, {})
            
            member_count = guild.member_count
            bot_count = sum(1 for member in guild.members if member.bot)
            human_count = member_count - bot_count
            
            # Update member count channel
            member_channel_id = stats_data.get('member_channel')
            if member_channel_id:
                channel = guild.get_channel(member_channel_id)
                if channel:
                    new_name = f"👥 Members: {human_count}"
                    if channel.name != new_name:
                        await channel.edit(name=new_name)
                        
            # Update bot count channel  
            bot_channel_id = stats_data.get('bot_channel')
            if bot_channel_id:
                channel = guild.get_channel(bot_channel_id)
                if channel:
                    new_name = f"🤖 Bots: {bot_count}"
                    if channel.name != new_name:
                        await channel.edit(name=new_name)
                        
        except discord.HTTPException as e:
            if e.status == 429:
                logger.warning(f"Rate limited updating stats for {guild.name}")
            else:
                logger.error(f"HTTP error updating stats: {e}")
        except Exception as e:
            logger.error(f"Error updating stats channels: {e}")
        
        await asyncio.sleep(0.1)  # Minimal rate limit protection

# --- BOT EVENTS ---
@bot.event
async def on_ready():
    # Start web server for hosting platforms
    web_server_thread = threading.Thread(target=web_server, daemon=True)
    web_server_thread.start()
    bot.start_time = datetime.now()

    print(f"✅ {bot.user} is now online!")
    print(f"📊 Connected to {len(bot.guilds)} guilds")
    print(f"👥 Serving {len(bot.users)} users")
    
    # Add persistent views
    bot.add_view(EnhancedTicketView())
    bot.add_view(AdvancedTicketView())
    bot.add_view(CustomerVouchView(user=None, product=None)) # Register with dummy data
    
    # Re-register persistent giveaway views from storage
    for guild_id_str, giveaways in data_manager.data['giveaways'].items():
        for msg_id, gw_info in giveaways.items():
            bot.add_view(GiveawayEntryView(gw_info['giveaway_id']))

    # Sync slash commands
    try:
        synced = await bot.tree.sync()
        print(f"🔄 Synced {len(synced)} slash commands globally")
        for command in synced:
            print(f"   /{command.name}")
    except Exception as e:
        print(f"❌ Failed to sync commands: {e}")

    # Emergency command registration check
    print("🔍 Checking command registration...")
    
    registered_commands = bot.tree.get_commands()
    command_names = [cmd.name for cmd in registered_commands]
    
    print(f"📝 Registered commands: {command_names}")
    
    if 'help' not in command_names:
        print("⚠️ WARNING: Help command not found in registered commands!")
    if 'ping' not in command_names:
        print("⚠️ WARNING: Ping command not found in registered commands!")
    
    print(f"📊 Total commands ready: {len(registered_commands)}")
        
    # Start background tasks
    if not check_temp_bans.is_running():
        check_temp_bans.start()
    if not check_giveaways.is_running():
        check_giveaways.start()
    if not backup_data_task.is_running():
        backup_data_task.start()
    if not update_stats_channels.is_running():
        update_stats_channels.start()
    print("🔄 All background tasks started successfully")

@bot.event
async def on_guild_join(guild):
    print(f"🎉 Joined new guild: {guild.name} (ID: {guild.id})")
    
    # Try to setup admin permissions
    success = await setup_bot_permissions(guild)
    
    if success:
        print(f"✅ Successfully setup permissions in {guild.name}")
    else:
        print(f"⚠️ Limited permissions in {guild.name} - some features may not work")
    
    # Send welcome message to owner or first available channel
    await send_welcome_message(guild)
    
    # Try to sync commands
    try:
        await bot.tree.sync()
        print(f"🔄 Re-synced commands for {guild.name}")
    except Exception as e:
        print(f"❌ Sync error for {guild.name}: {e}")

@bot.event
async def on_member_join(member):
    guild = member.guild
    guild_id = str(guild.id)
    
    # Auto-role assignment
    auto_role_ids = data_manager.data['auto_roles'].get(guild_id, [])
    if auto_role_ids:
        roles_to_add = []
        for role_id in auto_role_ids:
            role = guild.get_role(role_id)
            if role:
                roles_to_add.append(role)
        if roles_to_add:
            try:
                await member.add_roles(*roles_to_add, reason="Auto-role assignment")
            except discord.Forbidden:
                logger.warning(f"Cannot assign auto-roles to {member.name}")

    # Welcome message
    welcome_config = data_manager.data['welcome'].get(guild_id)
    if welcome_config and welcome_config.get("enabled", False):
        welcome_channel_id = welcome_config.get('channel_id')
        welcome_channel = guild.get_channel(welcome_channel_id)
        if not welcome_channel:
             # Fallback to default channel name if configured one is deleted
             welcome_channel = discord.utils.get(guild.text_channels, name=CHANNELS['WELCOME'])

        if welcome_channel:
            title = welcome_config["title"].replace("{server}", guild.name).replace("{user}", member.display_name).replace("{member_count}", str(guild.member_count))
            message = welcome_config["message"].replace("{server}", guild.name).replace("{user}", member.mention).replace("{member_count}", str(guild.member_count))
            
            welcome_embed = create_embed(
                title,
                message,
                welcome_config.get('color', CONFIG['MAIN_COLOR']),
                thumbnail=member.display_avatar.url,
                image=welcome_config.get('image_url')
            )
            
            try:
                await welcome_channel.send(embed=welcome_embed)
            except discord.Forbidden:
                logger.warning(f"Cannot send welcome message in {guild.name}")

@bot.event
async def on_member_remove(member):
    await log_to_channel(
        member.guild,
        f"👋 {member} ({member.id}) left the server",
        CHANNELS['MOD_LOG']
    )

@bot.event
async def on_message(message):
    if message.author.bot:
        return

    user_id_str = str(message.author.id)

    # AFK system: remove status on message
    if user_id_str in data_manager.data['afk']:
        afk_reason = data_manager.data['afk'].pop(user_id_str)
        data_manager.save_category_data('afk')
        try:
            await message.channel.send(f"✅ Welcome back, {message.author.mention}! Removed AFK status: `{afk_reason}`", delete_after=10)
        except discord.Forbidden:
            pass

    # AFK mentions - Process this regardless of whether user was AFK
    if message.mentions:
        for mentioned_user in message.mentions:
            mentioned_user_id_str = str(mentioned_user.id)
            if mentioned_user_id_str in data_manager.data['afk']:
                afk_reason = data_manager.data['afk'][mentioned_user_id_str]
                embed = create_embed(
                    f"💤 {mentioned_user.display_name} is AFK",
                    f"**Reason:** `{afk_reason}`",
                    CONFIG['WARNING_COLOR'],
                    thumbnail=mentioned_user.display_avatar.url
                )
                try:
                    await message.channel.send(embed=embed, delete_after=15)
                except discord.Forbidden:
                    pass
            
    await bot.process_commands(message)

@bot.event
async def on_app_command_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    error_msg = f"❌ An unexpected error occurred: {str(error)}"
    logger.error(f"Command error in '{interaction.command.name}' used by {interaction.user} in {interaction.guild.name if interaction.guild else 'DM'}: {error}")
    traceback.print_exc()
    
    try:
        if isinstance(error, app_commands.MissingPermissions):
            error_msg = f"❌ You don't have the required permissions: `{'`, `'.join(error.missing_permissions)}`"
        elif isinstance(error, app_commands.BotMissingPermissions):
            error_msg = f"❌ I don't have the required permissions: `{'`, `'.join(error.missing_permissions)}`"
        elif isinstance(error, app_commands.CommandOnCooldown):
            error_msg = f"⏳ This command is on cooldown. Please try again in {error.retry_after:.2f} seconds."

        if not interaction.response.is_done():
            await interaction.response.send_message(error_msg, ephemeral=True)
        else:
            await interaction.followup.send(error_msg, ephemeral=True)
    except Exception as e:
        logger.error(f"Error within the error handler: {e}")

# === SLASH COMMANDS ===

@bot.tree.command(name="quick_edit", description="Quick edit price, color, or image for a template")
@app_commands.describe(
    template="Template to edit",
    price="New price (use | to separate)",
    color="New hex color",
    image_url="New GIF/image URL"
)
@app_commands.checks.has_permissions(administrator=True)
async def quick_edit(interaction: discord.Interaction, template: str, price: str = None, color: str = None, image_url: str = None):
    if template not in data_manager.data['templates']:
        await interaction.response.send_message(f"❌ Template `{template}` not found.", ephemeral=True)
        return
    
    updated = []
    
    if price:
        data_manager.data['templates'][template]['price'] = price
        updated.append("price")
    
    if color:
        try:
            color_int = int(color.replace("#", ""), 16)
            data_manager.data['templates'][template]['color'] = color_int
            updated.append("color")
        except:
            pass
    
    if image_url:
        data_manager.data['templates'][template]['image_url'] = image_url
        updated.append("image")
    
    data_manager.save_category_data('templates')
    
    await interaction.response.send_message(
        f"✅ Updated {', '.join(updated)} for `{template}`",
        ephemeral=True
    )

# test your ticket panel

@bot.tree.command(name="test_ticket_panel", description="Test if your ticket panel is working correctly")
@app_commands.checks.has_permissions(administrator=True)
async def test_ticket_panel(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    
    guild_id = str(interaction.guild.id)
    ticket_config = data_manager.data['ticket_config'].get(guild_id, {})
    
    test_results = []
    
    # Test 1: Check if panel is configured
    if 'ticket_channel_id' in ticket_config and 'ticket_message_id' in ticket_config:
        channel_id = ticket_config['ticket_channel_id']
        message_id = ticket_config['ticket_message_id']
        
        # Test 2: Check if channel exists
        channel = interaction.guild.get_channel(channel_id)
        if channel:
            test_results.append("✅ Configured channel exists")
            
            # Test 3: Check if message exists
            try:
                message = await channel.fetch_message(message_id)
                test_results.append("✅ Configured message exists")
                
                # Test 4: Check if it has components (buttons/dropdowns)
                if message.components:
                    test_results.append("✅ Message has interactive components")
                else:
                    test_results.append("⚠️ Message has no interactive components")
                
                # Test 5: Check URL
                ticket_url = f"https://discord.com/channels/{interaction.guild.id}/{channel_id}/{message_id}"
                test_results.append(f"🔗 **Working URL:** {ticket_url}")
                
            except discord.NotFound:
                test_results.append("❌ Configured message not found")
            except discord.Forbidden:
                test_results.append("❌ No permission to access message")
        else:
            test_results.append("❌ Configured channel not found")
    else:
        test_results.append("❌ No ticket panel configured")
    
    # Auto-detection test
    found_panels = 0
    for channel in interaction.guild.text_channels:
        try:
            if not channel.permissions_for(interaction.guild.me).read_message_history:
                continue
                
            async for message in channel.history(limit=20):
                if (message.author == bot.user and 
                    message.embeds and 
                    message.components and
                    "ticket" in message.embeds[0].title.lower()):
                    found_panels += 1
                    break
        except:
            continue
    
    test_results.append(f"🔍 Auto-detected {found_panels} ticket panel(s)")
    
    # Create results embed
    status_color = CONFIG['SUCCESS_COLOR'] if "✅" in test_results[0] else CONFIG['ERROR_COLOR']
    
    embed = create_embed(
        "🧪 Ticket Panel Test Results",
        "Comprehensive testing of your ticket panel setup:",
        status_color,
        fields=[
            ("Test Results", "\n".join(test_results), False)
        ]
    )
    
    if found_panels == 0 and "❌" in test_results[0]:
        embed.add_field(
            name="🔧 Quick Fix",
            value="Run `/find_ticket_panel` to automatically configure your panel",
            inline=False
        )
    
    await interaction.followup.send(embed=embed, ephemeral=True)

@bot.tree.command(name="ticket_add_user", description="Add a user to an existing ticket")
@app_commands.describe(
    user="User to add to the ticket",
    ticket_channel="Ticket channel (leave empty to use current channel)"
)
@app_commands.checks.has_permissions(manage_channels=True)
async def ticket_add_user(interaction: discord.Interaction, user: discord.Member, ticket_channel: discord.TextChannel = None):
    """Add a user to an existing ticket channel"""
    
    # Use current channel if no ticket channel specified
    target_channel = ticket_channel or interaction.channel
    
    # Verify this is actually a ticket channel
    if not target_channel.name.startswith('ticket-'):
        await interaction.response.send_message("❌ This command can only be used in ticket channels.", ephemeral=True)
        return
    
    # Check if channel has ticket topic
    if not target_channel.topic or "Creator ID:" not in target_channel.topic:
        await interaction.response.send_message("❌ This doesn't appear to be a valid ticket channel.", ephemeral=True)
        return
    
    # Check if user is already in the ticket
    existing_overwrites = target_channel.overwrites
    if user in existing_overwrites:
        await interaction.response.send_message(f"❌ {user.mention} already has access to this ticket.", ephemeral=True)
        return
    
    try:
        # Add user permissions to the ticket
        overwrites = discord.PermissionOverwrite(
            view_channel=True,
            send_messages=True,
            read_message_history=True,
            attach_files=True,
            embed_links=True
        )
        
        await target_channel.set_permissions(user, overwrite=overwrites, reason=f"Added to ticket by {interaction.user}")
        
        # Create success embed
        embed = create_embed(
            "✅ User Added to Ticket",
            f"**Added:** {user.mention}\n**Channel:** {target_channel.mention}\n**Added by:** {interaction.user.mention}",
            CONFIG['SUCCESS_COLOR'],
            thumbnail=user.display_avatar.url,
            fields=[
                ("🎫 Ticket Channel", target_channel.mention, True),
                ("👤 Added User", f"{user.mention}\n`{user.name}#{user.discriminator}`", True),
                ("⏰ Added At", f"<t:{int(datetime.now().timestamp())}:F>", True)
            ]
        )
        
        # Send confirmation to command user
        await interaction.response.send_message(embed=embed, ephemeral=True)
        
        # Send notification in the ticket channel
        notification_embed = create_embed(
            "👥 User Added to Ticket",
            f"{user.mention} has been added to this ticket by {interaction.user.mention}",
            CONFIG['MAIN_COLOR']
        )
        
        await target_channel.send(embed=notification_embed)
        
        # Log the action
        await log_to_channel(
            interaction.guild,
            f"👥 {interaction.user} added {user} to ticket {target_channel.mention}",
            CHANNELS['LOG']
        )
        
    except discord.Forbidden:
        await interaction.response.send_message("❌ I don't have permission to modify this channel's permissions.", ephemeral=True)
    except Exception as e:
        logger.error(f"Error adding user to ticket: {e}")
        await interaction.response.send_message("❌ An error occurred while adding the user to the ticket.", ephemeral=True)

@bot.tree.command(name="ticket_remove_user", description="Remove a user from an existing ticket")
@app_commands.describe(
    user="User to remove from the ticket",
    ticket_channel="Ticket channel (leave empty to use current channel)"
)
@app_commands.checks.has_permissions(manage_channels=True)
async def ticket_remove_user(interaction: discord.Interaction, user: discord.Member, ticket_channel: discord.TextChannel = None):
    """Remove a user from an existing ticket channel"""
    
    # Use current channel if no ticket channel specified
    target_channel = ticket_channel or interaction.channel
    
    # Verify this is actually a ticket channel
    if not target_channel.name.startswith('ticket-'):
        await interaction.response.send_message("❌ This command can only be used in ticket channels.", ephemeral=True)
        return
    
    # Check if channel has ticket topic
    if not target_channel.topic or "Creator ID:" not in target_channel.topic:
        await interaction.response.send_message("❌ This doesn't appear to be a valid ticket channel.", ephemeral=True)
        return
    
    # Don't allow removing the ticket creator
    try:
        creator_id = int(target_channel.topic.split("Creator ID: ")[1].split(" |")[0])
        if user.id == creator_id:
            await interaction.response.send_message("❌ Cannot remove the ticket creator from their own ticket.", ephemeral=True)
            return
    except (ValueError, IndexError):
        pass
    
    # Check if user has access to the ticket
    existing_overwrites = target_channel.overwrites
    if user not in existing_overwrites:
        await interaction.response.send_message(f"❌ {user.mention} doesn't have special access to this ticket.", ephemeral=True)
        return
    
    try:
        # Remove user permissions from the ticket
        await target_channel.set_permissions(user, overwrite=None, reason=f"Removed from ticket by {interaction.user}")
        
        # Create success embed
        embed = create_embed(
            "✅ User Removed from Ticket",
            f"**Removed:** {user.mention}\n**Channel:** {target_channel.mention}\n**Removed by:** {interaction.user.mention}",
            CONFIG['WARNING_COLOR'],
            thumbnail=user.display_avatar.url,
            fields=[
                ("🎫 Ticket Channel", target_channel.mention, True),
                ("👤 Removed User", f"{user.mention}\n`{user.name}#{user.discriminator}`", True),
                ("⏰ Removed At", f"<t:{int(datetime.now().timestamp())}:F>", True)
            ]
        )
        
        # Send confirmation to command user
        await interaction.response.send_message(embed=embed, ephemeral=True)
        
        # Send notification in the ticket channel
        notification_embed = create_embed(
            "👥 User Removed from Ticket",
            f"{user.mention} has been removed from this ticket by {interaction.user.mention}",
            CONFIG['WARNING_COLOR']
        )
        
        await target_channel.send(embed=notification_embed)
        
        # Log the action
        await log_to_channel(
            interaction.guild,
            f"👥 {interaction.user} removed {user} from ticket {target_channel.mention}",
            CHANNELS['LOG']
        )
        
    except discord.Forbidden:
        await interaction.response.send_message("❌ I don't have permission to modify this channel's permissions.", ephemeral=True)
    except Exception as e:
        logger.error(f"Error removing user from ticket: {e}")
        await interaction.response.send_message("❌ An error occurred while removing the user from the ticket.", ephemeral=True)

@bot.tree.command(name="ticket_list_users", description="List all users with access to a ticket")
@app_commands.describe(ticket_channel="Ticket channel (leave empty to use current channel)")
@app_commands.checks.has_permissions(manage_channels=True)
async def ticket_list_users(interaction: discord.Interaction, ticket_channel: discord.TextChannel = None):
    """List all users with access to the current ticket"""
    
    # Use current channel if no ticket channel specified
    target_channel = ticket_channel or interaction.channel
    
    # Verify this is actually a ticket channel
    if not target_channel.name.startswith('ticket-'):
        await interaction.response.send_message("❌ This command can only be used in ticket channels.", ephemeral=True)
        return
    
    # Check if channel has ticket topic
    if not target_channel.topic or "Creator ID:" not in target_channel.topic:
        await interaction.response.send_message("❌ This doesn't appear to be a valid ticket channel.", ephemeral=True)
        return
    
    try:
        # Get ticket creator ID
        creator_id = int(target_channel.topic.split("Creator ID: ")[1].split(" |")[0])
        creator = interaction.guild.get_member(creator_id)
        
        # Get all users with special permissions
        users_with_access = []
        overwrites = target_channel.overwrites
        
        for entity, permissions in overwrites.items():
            if isinstance(entity, discord.Member) and permissions.view_channel:
                role_info = "Creator" if entity.id == creator_id else "Added User"
                users_with_access.append(f"• {entity.mention} - *{role_info}*")
        
        # Also check for roles that have access
        roles_with_access = []
        for entity, permissions in overwrites.items():
            if isinstance(entity, discord.Role) and permissions.view_channel and entity != interaction.guild.default_role:
                roles_with_access.append(f"• {entity.mention}")
        
        embed = create_embed(
            f"👥 Ticket Access List",
            f"Users and roles with access to {target_channel.mention}",
            CONFIG['MAIN_COLOR'],
            fields=[
                ("🎫 Ticket Info", f"**Channel:** {target_channel.mention}\n**Creator:** {creator.mention if creator else 'Unknown'}", False)
            ]
        )
        
        if users_with_access:
            embed.add_field(
                name=f"👤 Users with Access ({len(users_with_access)})",
                value="\n".join(users_with_access),
                inline=False
            )
        
        if roles_with_access:
            embed.add_field(
                name=f"🏷️ Roles with Access ({len(roles_with_access)})",
                value="\n".join(roles_with_access),
                inline=False
            )
        
        if not users_with_access and not roles_with_access:
            embed.add_field(
                name="ℹ️ Access Info",
                value="Only the bot, server admins, and support roles have access to this ticket.",
                inline=False
            )
        
        await interaction.response.send_message(embed=embed, ephemeral=True)
        
    except (ValueError, IndexError) as e:
        logger.error(f"Error parsing ticket creator ID: {e}")
        await interaction.response.send_message("❌ Error reading ticket information.", ephemeral=True)
    except Exception as e:
        logger.error(f"Error listing ticket users: {e}")
        await interaction.response.send_message("❌ An error occurred while listing ticket users.", ephemeral=True)

# Add this simple test command:
@bot.tree.command(name="ping", description="Check bot latency and response time")
async def ping(interaction: discord.Interaction):
    """Check bot latency"""
    start_time = time.time()
    
    # Calculate WebSocket latency
    ws_latency = bot.latency * 1000  # Convert to milliseconds
    
    await interaction.response.defer()
    
    # Calculate API response time
    end_time = time.time()
    api_latency = (end_time - start_time) * 1000
    
    # Determine connection quality
    if ws_latency < 100:
        status_emoji = "🟢"
        status_text = "Excellent"
        color = CONFIG['SUCCESS_COLOR']
    elif ws_latency < 200:
        status_emoji = "🟡"
        status_text = "Good"
        color = CONFIG['WARNING_COLOR']
    else:
        status_emoji = "🔴"
        status_text = "Poor"
        color = CONFIG['ERROR_COLOR']
    
    embed = create_embed(
        f"{status_emoji} Bot Status - {status_text}",
        f"Connection quality and response times",
        color,
        fields=[
            ("🏓 WebSocket Latency", f"{ws_latency:.0f}ms", True),
            ("⚡ API Response", f"{api_latency:.0f}ms", True),
            ("📊 Status", f"{status_emoji} {status_text}", True),
            ("🕐 Uptime", f"<t:{int((datetime.now() - bot.start_time).total_seconds())}:T>" if hasattr(bot, 'start_time') else "Unknown", True),
            ("🌐 Servers", f"{len(bot.guilds)}", True),
            ("👥 Users", f"{len(bot.users)}", True)
        ],
        thumbnail=bot.user.display_avatar.url if bot.user else None
    )
    
    await interaction.followup.send(embed=embed)

# -----Debug Command to check command registration---

# Add this debug command to see what commands are registered:
@bot.tree.command(name="debug_commands", description="🔧 Debug command registration (Admin only)")
@app_commands.checks.has_permissions(administrator=True)
async def debug_commands(interaction: discord.Interaction):
    """Debug command registration issues"""
    
    await interaction.response.defer(ephemeral=True)
    
    # Get all registered commands
    global_commands = bot.tree.get_commands()
    guild_commands = bot.tree.get_commands(guild=interaction.guild)
    
    # Create debug embed
    embed = create_embed(
        "🔧 Command Debug Information",
        "Analyzing command registration status...",
        CONFIG['WARNING_COLOR']
    )
    
    # Global commands info
    global_cmd_names = [cmd.name for cmd in global_commands]
    embed.add_field(
        name="🌐 Global Commands",
        value=f"**Count:** {len(global_commands)}\n**Names:** {', '.join(global_cmd_names[:10])}" + 
              (f"... (+{len(global_cmd_names)-10} more)" if len(global_cmd_names) > 10 else ""),
        inline=False
    )
    
    # Guild commands info
    guild_cmd_names = [cmd.name for cmd in guild_commands]
    embed.add_field(
        name=f"🏠 Guild Commands ({interaction.guild.name})",
        value=f"**Count:** {len(guild_commands)}\n**Names:** {', '.join(guild_cmd_names[:10])}" + 
              (f"... (+{len(guild_cmd_names)-10} more)" if len(guild_cmd_names) > 10 else ""),
        inline=False
    )
    
    # Check specific commands
    help_registered = any(cmd.name == 'help' for cmd in global_commands + guild_commands)
    ping_registered = any(cmd.name == 'ping' for cmd in global_commands + guild_commands)
    
    embed.add_field(
        name="🔍 Command Status Check",
        value=f"**Help Command:** {'✅ Registered' if help_registered else '❌ Missing'}\n"
              f"**Ping Command:** {'✅ Registered' if ping_registered else '❌ Missing'}\n"
              f"**Total Available:** {len(global_commands + guild_commands)}",
        inline=False
    )
    
    # Sync status
    last_sync = None   
    sync_status = f"<t:{int(last_sync)}:R>" if last_sync else "Never synced"
    
    embed.add_field(
        name="⚙️ Sync Information", 
        value=f"**Last Sync:** {sync_status}\n"
              f"**Sync Manager:** ❌ Not initialized\n"
              f"**Bot Ready:** {'✅ Yes' if bot.is_ready() else '❌ No'}",
        inline=False
    )
    
    await interaction.followup.send(embed=embed, ephemeral=True)

# -----Force Sync Command------

# Add this temporary command to force sync NOW:
@bot.tree.command(name="emergency_sync", description="Emergency command sync (Admin only)")
@app_commands.checks.has_permissions(administrator=True)
async def emergency_sync(interaction: discord.Interaction):
    """Emergency command synchronization"""
    await interaction.response.defer(ephemeral=True)
    
    try:
        logger.info(f"🚨 Emergency sync requested by {interaction.user} in {interaction.guild.name}")
        
        # Sync commands
        synced = await bot.tree.sync()
        
        embed = create_embed(
            "✅ Emergency Sync Complete",
            f"Successfully synced {len(synced)} commands.",
            CONFIG['SUCCESS_COLOR'],
            fields=[
                ("📊 Synced Commands", str(len(synced)), True),
                ("👤 Requested By", interaction.user.mention, True),
                ("⏰ Sync Time", f"<t:{int(time.time())}:R>", True)
            ]
        )
        
        await interaction.followup.send(embed=embed)
        logger.info(f"✅ Emergency sync completed successfully - {len(synced)} commands")
        
    except Exception as e:
        error_msg = f"❌ Emergency sync failed: {str(e)}"
        logger.error(error_msg)
        
        embed = create_embed(
            "❌ Sync Failed",
            f"Emergency sync encountered an error:\n```{str(e)}```",
            CONFIG['ERROR_COLOR']
        )
        
        await interaction.followup.send(embed=embed)

@bot.tree.command(name="help", description="Show all available commands and features")
async def help_command(interaction: discord.Interaction):
    """Comprehensive help command"""
    
    embed = create_embed(
        f"📚 {bot.user.name} Help & Commands",
        f"Complete guide to using {bot.user.name}",
        CONFIG['MAIN_COLOR'],
        thumbnail=bot.user.display_avatar.url
    )
    
    # Command categories
    categories = {
        "🚀 **Setup Commands**": [
            "`/setup_server` - Complete server setup",
            "`/setup_logs` - Configure private logging", 
            "`/setup_branding` - Set company branding",
            "`/ticket` - Setup support ticket system"
        ],
        "💰 **Business Commands**": [
            "`/confirm_payment` - Deliver product keys",
            "`/generate_invoice` - Create invoices",
            "`/add_keys` - Add product license keys",
            "`/check_keys` - View key inventory"
        ],
        "🎫 **Support System**": [
            "`/ticket` - Open ticket panel",
            "`/setup_ticket_support_role` - Set support role",
            "`/ticket_add_user` - Add user to ticket",
            "`/ticket_remove_user` - Remove user from ticket", 
            "`/ticket_list_users` - List ticket users"
        ],
        
        "🛡️ **Moderation**": [
            "`/warn` - Warn users",
            "`/tempban` - Temporary ban",
            "`/mute` - Timeout users",
            "`/clear_messages` - Bulk delete messages"
        ],
        "🎁 **Giveaways**": [
            "`/giveaway` - Start giveaways",
            "`/end_giveaway` - End early",
            "`/reroll_giveaway` - Reroll winners"
        ],
        "📊 **Analytics**": [
            "`/server_stats` - Server statistics",
            "`/customer_dashboard` - Customer info",
            "`/invoice_dashboard` - Sales dashboard"
        ]
    }
    
    for category, commands in categories.items():
        embed.add_field(
            name=category,
            value="\n".join(commands),
            inline=False
        )
    
    embed.add_field(
        name="🔗 **Useful Links**",
        value=f"• Use `/invite` to get bot invite link\n• Use `/payment` to show payment methods\n• Use `/create_tos` for Terms of Service",
        inline=False
    )
    
    embed.add_field(
        name="⚠️ **Important Notes**",
        value="• Most commands require Administrator permissions\n• Use `/setup_server` first for best experience\n• Contact support if you need help",
        inline=False
    )
    
    await interaction.response.send_message(embed=embed, ephemeral=True)

# --- Setup logs----

@bot.tree.command(name="setup_logs", description="Creates a private category and moves all log channels into it.")
@app_commands.describe(admin_role="The only role (besides admins) that can see the log channels.")
@app_commands.checks.has_permissions(administrator=True)
async def setup_logs(interaction: discord.Interaction, admin_role: discord.Role):
    await interaction.response.defer(ephemeral=True)
    
    guild = interaction.guild
    log_category_name = "🔒 Bot Logs"
    
    # Define permissions for the private category
    overwrites = {
        guild.default_role: discord.PermissionOverwrite(view_channel=False),
        guild.me: discord.PermissionOverwrite(view_channel=True),
        admin_role: discord.PermissionOverwrite(view_channel=True)
    }
    
    # Get or create the private log category
    log_category = discord.utils.get(guild.categories, name=log_category_name)
    if not log_category:
        try:
            log_category = await guild.create_category(log_category_name, overwrites=overwrites)
        except discord.Forbidden:
            await interaction.followup.send("❌ I don't have permission to create categories.")
            return
    else:
        # If category already exists, just update its permissions
        await log_category.edit(overwrites=overwrites)

    # Save the admin role ID for future use
    guild_id = str(guild.id)
    if guild_id not in data_manager.data.get('log_config', {}):
        data_manager.data['log_config'][guild_id] = {}
    data_manager.data['log_config'][guild_id]['admin_role_id'] = admin_role.id
    data_manager.save_category_data('log_config')
    
    # Find and move all existing log channels
    log_channel_names = [
        CHANNELS['LOG'], CHANNELS['TRANSCRIPT'], CHANNELS['VOUCH'],
        CHANNELS['MOD_LOG'], CHANNELS['GIVEAWAY_LOGS']
    ]
    
    moved_channels = []
    for channel_name in log_channel_names:
        channel = discord.utils.get(guild.text_channels, name=channel_name)
        if channel and (not channel.category or channel.category.id != log_category.id):
            try:
                await channel.edit(category=log_category)
                moved_channels.append(channel.mention)
            except discord.Forbidden:
                await interaction.followup.send(f"⚠️ Could not move {channel.mention}, I lack permissions.")
    
    embed = create_embed(
        "✅ Logs Setup Complete",
        f"The **{log_category.name}** category is now private and only visible to you and {admin_role.mention}.",
        CONFIG['SUCCESS_COLOR'],
        fields=[("Moved Channels", ", ".join(moved_channels) if moved_channels else "None", False)]
    )
    await interaction.followup.send(embed=embed)

#---Setup branding Command----
@bot.tree.command(name="setup_branding", description="Configure company branding for invoices and templates")
@app_commands.describe(
    logo_url="URL to your company logo (square format recommended)",
    banner_url="URL to your invoice banner image",
    primary_color="Hex color code (e.g., #00CED1)",
    company_name="Your company name",
    footer_text="Text to show at the bottom of embeds"
)
@app_commands.checks.has_permissions(administrator=True)
async def setup_branding(
    interaction: discord.Interaction,
    logo_url: str = None,
    banner_url: str = None,
    primary_color: str = None,
    company_name: str = None,
    footer_text: str = None
):
    try:
        # Ensure branding data exists
        if 'branding' not in data_manager.data:
            data_manager.data['branding'] = get_branding_data()
        
        # Update only provided values
        if logo_url:
            data_manager.data['branding']['logo_url'] = logo_url
        if banner_url:
            data_manager.data['branding']['banner_url'] = banner_url
        if primary_color:
            try:
                # Validate hex color
                color_int = int(primary_color.replace('#', ''), 16)
                data_manager.data['branding']['primary_color'] = color_int
            except ValueError:
                await interaction.response.send_message("❌ Invalid color format. Use hex format like #00CED1", ephemeral=True)
                return
        if company_name:
            data_manager.data['branding']['company_name'] = company_name
        if footer_text:
            data_manager.data['branding']['footer_text'] = footer_text
            
        data_manager.save_category_data('branding')
        
        # Show preview with current branding
        branding = get_branding_data()
        preview_embed = create_embed(
            f" {branding['company_name']} Branding",
            "Your branding has been updated successfully. Here's how it looks:",
            branding['primary_color'],
            thumbnail=branding['logo_url'],
            image=branding['banner_url'],
            footer=branding['footer_text']
        )
        
        await interaction.response.send_message(embed=preview_embed, ephemeral=True)
        
    except Exception as e:
        logger.error(f"Error in setup_branding: {e}")
        await interaction.response.send_message("❌ An error occurred while updating branding. Please try again.", ephemeral=True)

# ---Command to view users Invoice----
@bot.tree.command(name="invoices", description="View a user's purchase invoices")
@app_commands.describe(user="User to view invoices for")
@app_commands.checks.has_permissions(administrator=True)
async def view_invoices(interaction: discord.Interaction, user: discord.Member = None):
    await interaction.response.defer(ephemeral=True)
    
    target_user = user or interaction.user
    guild_id = str(interaction.guild.id)
    
    if guild_id not in data_manager.data['invoices']:
        await interaction.followup.send(f"❌ No invoices found for this server.", ephemeral=True)
        return
    
    # Find all invoices for the user
    user_invoices = []
    for invoice_id, invoice_data in data_manager.data['invoices'][guild_id].items():
        if invoice_data.get('customer_id') == target_user.id:
            user_invoices.append(invoice_data)
    
    if not user_invoices:
        await interaction.followup.send(f"❌ No invoices found for {target_user.mention}.", ephemeral=True)
        return
    
    # Sort by timestamp (newest first)
    user_invoices.sort(key=lambda x: x.get('timestamp', 0), reverse=True)
    
    embed = create_embed(
        f"📄 Invoices for {target_user.display_name}",
        f"Found {len(user_invoices)} invoices for this user.",
        CONFIG['MAIN_COLOR'],
        thumbnail=target_user.display_avatar.url
    )
    
    # Add most recent 10 invoices
    for invoice in user_invoices[:10]:
        embed.add_field(
            name=f"Invoice #{invoice['invoice_id']}",
            value=f"**Product:** {invoice['product']}\n**Date:** <t:{invoice['timestamp']}:F>\n**Processed by:** <@{invoice['processor_id']}>",
            inline=False
        )
    
    await interaction.followup.send(embed=embed, ephemeral=True)

# ---Standalone Invoice Generating Command-----
@bot.tree.command(name="generate_invoice", description="Generate a branded invoice for a customer")
@app_commands.describe(
    user="User to generate invoice for", 
    product="Product purchased", 
    amount="Amount paid",
    template="Invoice template to use (optional)"
)
@app_commands.checks.has_permissions(administrator=True)
async def generate_invoice_command(
    interaction: discord.Interaction, 
    user: discord.Member, 
    product: str, 
    amount: float, 
    template: str = "default"
):
    await interaction.response.defer(ephemeral=True)
    
    try:
        # Generate invoice number and timestamp
        invoice_num = random.randint(10000, 99999)
        timestamp = int(datetime.now().timestamp())
        
        # Get template data
        guild_id = str(interaction.guild.id)
        branding = get_branding_data()  # Use safe function
        
        if (template != "default" and 
            guild_id in data_manager.data['invoice_templates'] and 
            template in data_manager.data['invoice_templates'][guild_id]):
            # Use selected template
            template_data = data_manager.data['invoice_templates'][guild_id][template]
        else:
            # Use default template
            template_data = {
                'title': f"📄 {branding['company_name']} Invoice #{invoice_num}",
                'description': f"**Product:** {product}\n**Customer:** {user.mention}\n**Processed by:** {interaction.user.mention}",
                'custom_fields': {},
                'color': branding['primary_color']
            }
        
        # Format template placeholders
        title = template_data['title'].replace("{invoice_id}", str(invoice_num)).replace("{product}", product)
        description = template_data['description'].replace("{product}", product).replace("{customer}", user.mention).replace("{processor}", interaction.user.mention)
        
        # Create fields list
        fields = [
            ("📅 Date", f"<t:{timestamp}:F>", True),
            ("🆔 Customer ID", f"`{user.id}`", True),
            ("💰 Amount", f"${amount:.2f}", True),
            ("✅ Status", "Completed", True)
        ]
        
        # Add custom fields from template
        for field_name, field_value in template_data.get('custom_fields', {}).items():
            fields.append((field_name, field_value, True))
        
        # Create invoice embed
        invoice_embed = create_embed(
            title,
            description,
            template_data.get('color', branding['primary_color']),
            fields=fields,
            thumbnail=branding['logo_url'],
            image=branding['banner_url'],
            footer=branding['footer_text']
        )
        
        # Store invoice data
        invoice_data = {
            "invoice_id": invoice_num,
            "product": product,
            "customer_id": user.id,
            "customer_tag": f"{user.name}#{user.discriminator}",
            "processor_id": interaction.user.id,
            "processor_tag": f"{interaction.user.name}#{interaction.user.discriminator}",
            "amount": amount,
            "timestamp": timestamp,
            "template_used": template,
            "guild_id": interaction.guild.id
        }
        
        # Save to data storage
        if guild_id not in data_manager.data['invoices']:
            data_manager.data['invoices'][guild_id] = {}
        
        data_manager.data['invoices'][guild_id][str(invoice_num)] = invoice_data
        data_manager.save_category_data('invoices')
        
        # Send invoice to DM
        dm_status = "❌ Could not send to DMs"
        try:
            await user.send(embed=invoice_embed)
            dm_status = "✅ Sent to DMs"
        except discord.Forbidden:
            dm_status = "❌ Could not send to DMs (DMs closed)"
        except Exception as e:
            logger.error(f"Error sending invoice to DM: {e}")
            dm_status = "❌ Error sending to DMs"
        
        # Log to invoice channel (NEW FEATURE)
        invoice_logged = await log_invoice_to_channel(interaction.guild, invoice_data, invoice_embed)
        
        # Save to transcript channel (existing functionality)
        try:
            transcript_channel = await find_or_create_channel(interaction.guild, CHANNELS['TRANSCRIPT'])
            if transcript_channel:
                await transcript_channel.send(
                    content=f"📄 **New Invoice Generated:** #{invoice_num} for {user.mention}",
                    embed=invoice_embed
                )
        except Exception as e:
            logger.error(f"Error saving invoice to transcript channel: {e}")
        
        # Confirm to the admin
        confirmation_embed = create_embed(
            "✅ Invoice Generated Successfully",
            f"Invoice #{invoice_num} for {user.mention} has been generated and logged.",
            CONFIG['SUCCESS_COLOR'],
            fields=[
                ("📦 Product", product, True),
                ("💰 Amount", f"${amount:.2f}", True),
                ("🖼️ Template", template, True),
                ("💬 DM Status", dm_status, True),
                ("📋 Invoice Channel", "✅ Logged" if invoice_logged else "❌ Failed", True)
            ]
        )
        await interaction.followup.send(embed=confirmation_embed, ephemeral=True)
        
    except Exception as e:
        logger.error(f"Error in generate_invoice_command: {e}")
        await interaction.followup.send("❌ An error occurred while generating the invoice. Please try again.", ephemeral=True)

# ---Invoice channel management commands---
@bot.tree.command(name="setup_invoice_channel", description="Configure or recreate the invoice logging channel")
@app_commands.describe(channel="Specific channel to use for invoices (optional)")
@app_commands.checks.has_permissions(administrator=True)
async def setup_invoice_channel(interaction: discord.Interaction, channel: discord.TextChannel = None):
    await interaction.response.defer(ephemeral=True)
    
    try:
        if channel:
            # Use the specified channel
            invoice_channel = channel
            setup_type = "configured"
        else:
            # Create or find the default invoice channel
            invoice_channel = await find_or_create_channel(interaction.guild, CHANNELS['INVOICES'])
            setup_type = "created/found"
        
        if not invoice_channel:
            await interaction.followup.send("❌ Failed to create or access the invoice channel.", ephemeral=True)
            return
        
        # Send a setup message to the invoice channel
        setup_embed = create_embed(
            "📋 Invoice Channel Initialized",
            f"This channel will automatically log all generated invoices for {interaction.guild.name}.",
            CONFIG['SUCCESS_COLOR'],
            fields=[
                ("🔧 Configured by", interaction.user.mention, True),
                ("📅 Setup Date", f"<t:{int(datetime.now().timestamp())}:F>", True),
                ("⚙️ Auto-logging", "Enabled", True)
            ],
            thumbnail=get_branding_data()['logo_url'],
            footer="Invoice Logging System • All invoices will appear here"
        )
        
        await invoice_channel.send(embed=setup_embed)
        
        # Confirm to admin
        confirmation_embed = create_embed(
            "✅ Invoice Channel Setup Complete",
            f"Invoice logging has been {setup_type} successfully!",
            CONFIG['SUCCESS_COLOR'],
            fields=[
                ("📋 Channel", invoice_channel.mention, True),
                ("🔧 Setup Type", setup_type.title(), True),
                ("📝 Note", "All future invoices will be automatically logged here", False)
            ]
        )
        
        await interaction.followup.send(embed=confirmation_embed, ephemeral=True)
        
    except Exception as e:
        logger.error(f"Error setting up invoice channel: {e}")
        await interaction.followup.send("❌ An error occurred while setting up the invoice channel.", ephemeral=True)

# --- TICKET SYSTEM COMMANDS ---
@bot.tree.command(name="ticket", description="Open the enhanced ticket panel")
@app_commands.checks.has_permissions(manage_channels=True)
async def ticket_panel(interaction: discord.Interaction):
    embed = create_embed(
        "🎫 Ticket System",
        "**Need assistance?** Select your issue category below to create a support ticket.\n\n"
        "**Available Categories:**\n"
        
        "💳 **Purchase** - Buy products or services\n"

        "🔄 **Exchange** - Exchange Ticket\n" 
        
        "💬 **Support** - General help and support\n"
        
        "🤝 **Reseller Apply** - Join our reseller program\n"
        
        "🖼️ **Media** - Content and media requests\n"
        
        "🎁 **Giveaway** - Giveaway related inquiries\n\n"
        
        "⚡ **Fast Response:** Be detailed in your first message!",
        CONFIG['MAIN_COLOR'],
        image="https://media.discordapp.net/attachments/1162388547211370526/1403113823837225082/Copilot_20250805_220724.png?ex=68b75494&is=68b60314&hm=ddee0f6d0f8fadbc35f11f53071c8a4be2fbfc06a4f5b6cfa32e734ca0613de2&=&format=webp&quality=lossless&width=1240&height=826",
        thumbnail=interaction.guild.icon.url if interaction.guild.icon else None
    )
    
    await interaction.response.send_message(embed=embed, view=EnhancedTicketView())

@bot.tree.command(name="setup_ticket_support_role", description="Set the role to ping for new tickets")
@app_commands.describe(role="The support role to ping when tickets are created")
@app_commands.checks.has_permissions(administrator=True)
async def setup_ticket_support_role(interaction: discord.Interaction, role: discord.Role):
    guild_id = str(interaction.guild.id)
    if guild_id not in data_manager.data['ticket_config']:
        data_manager.data['ticket_config'][guild_id] = {}
    
    data_manager.data['ticket_config'][guild_id]['support_role_id'] = role.id
    data_manager.save_category_data('ticket_config')
    
    embed = create_embed(
        "✅ Support Role Configured",
        f"Support role set to {role.mention}\n\nThis role will be pinged when new tickets are created.",
        CONFIG['SUCCESS_COLOR']
    )
    await interaction.response.send_message(embed=embed, ephemeral=True)

# ---Delete temp Command---
@bot.tree.command(name="delete_product_template", description="Delete an existing product template.")
@app_commands.describe(template="The name of the template you want to delete.")
@app_commands.checks.has_permissions(administrator=True)
async def delete_product_template(interaction: discord.Interaction, template: str):
    if template in data_manager.data['templates']:
        del data_manager.data['templates'][template]
        data_manager.save_category_data('templates')
        await interaction.response.send_message(f"✅ Successfully deleted the product template: `{template}`", ephemeral=True)
    else:
        await interaction.response.send_message(f"❌ No template with the name `{template}` was found.", ephemeral=True)

@delete_product_template.autocomplete('template')
async def delete_product_template_autocomplete(
    interaction: discord.Interaction,
    current: str,
) -> List[app_commands.Choice[str]]:
    templates = data_manager.data.get('templates', {}).keys()
    return [
        app_commands.Choice(name=template, value=template)
        for template in templates if current.lower() in template.lower()
    ]

# --- KEY DELIVERY SYSTEM ---
@bot.tree.command(name="confirm_payment", description="Confirm payment and deliver product key")
@app_commands.describe(
    user="User who made the payment",
    product="Product name", 
    amount="Amount paid (for tier calculation)",
    template="DM template to use (optional)"
)
@app_commands.checks.has_permissions(administrator=True)
async def confirm_payment(interaction: discord.Interaction, user: discord.User, product: str, amount: float = 0.0, template: str = "default"):
    # Get the stock count FIRST
    stock = await key_manager.get_product_stock(product)
    
    embed = create_embed(
        "🔑 Payment Confirmation & Key Delivery",
        f"**Customer:** {user.mention}\n**Product:** {product}\n**Amount:** ${amount:.2f}",
        CONFIG['MAIN_COLOR'],
        fields=[
            # Now use the variable, not the coroutine
            ("📦 Product Stock", f"{stock} keys available", True),
            ("💰 Amount", f"${amount:.2f}", True),
            ("🎯 Template", template, True)
        ]
    )
    
    view = EnhancedDeliverKeyView(user, product, amount)
    await interaction.response.send_message(embed=embed, view=view, ephemeral=True)

@bot.tree.command(name="setup_dm_template", description="Create a custom DM template for key delivery")
@app_commands.checks.has_permissions(administrator=True)
async def setup_dm_template(interaction: discord.Interaction):
    await interaction.response.send_modal(DMTemplateModal())

@bot.tree.command(name="check_keys", description="Check product key inventory")
@app_commands.describe(product="Specific product to check (optional)")
@app_commands.checks.has_permissions(administrator=True) 
async def check_keys(interaction: discord.Interaction, product: str = None):
    await interaction.response.defer(ephemeral=True)
    
    if product:
        stock = await key_manager.get_product_stock(product)
        status_emoji = "✅" if stock > 10 else "⚠️" if stock > 0 else "❌"
        status_text = "Good Stock" if stock > 10 else "Low Stock" if stock > 0 else "OUT OF STOCK"
        color = CONFIG['SUCCESS_COLOR'] if stock > 10 else CONFIG['WARNING_COLOR'] if stock > 0 else CONFIG['ERROR_COLOR']
        
        embed = create_embed(
            f"{status_emoji} Stock Report: {product}",
            f"**Available Keys:** {stock}\n**Status:** {status_text}",
            color,
            fields=[("⏰ Last Updated", f"<t:{int(datetime.now().timestamp())}:R>", True)]
        )
    else:
        all_stock = await key_manager.get_product_stock()
        embed = create_embed(
            "📦 Complete Stock Overview",
            f"Total products: {len(all_stock)}" if all_stock else "No products in database",
            CONFIG['MAIN_COLOR']
        )
        
        if all_stock:
            sorted_products = sorted(all_stock.items(), key=lambda x: x[1])
            for product_name, count in sorted_products:
                status_emoji = "✅" if count > 10 else "⚠️" if count > 0 else "❌"
                status_text = "Good" if count > 10 else "Low" if count > 0 else "EMPTY"
                embed.add_field(
                    name=f"{status_emoji} {product_name}",
                    value=f"{count} keys\n*{status_text}*",
                    inline=True
                )
    
    await interaction.followup.send(embed=embed, ephemeral=True)

@bot.tree.command(name="add_keys", description="Add license keys to a product")
@app_commands.describe(product="Product name", keys="Comma-separated keys")
@app_commands.checks.has_permissions(administrator=True)
async def add_keys(interaction: discord.Interaction, product: str, keys: str):
    await interaction.response.defer(ephemeral=True)
    
    key_list = [key.strip() for key in keys.split(",") if key.strip()]
    
    if not key_list:
        await interaction.followup.send("❌ No valid keys provided.", ephemeral=True)
        return
    
    if len(key_list) > 100:
        await interaction.followup.send("❌ Maximum 100 keys per command.", ephemeral=True)
        return
    
    result = await key_manager.add_keys_to_product(product, key_list)
    
    if result['success']:
        # Get stock count first
        new_stock = await key_manager.get_product_stock(product)
        
        embed = create_embed(
            "✅ Keys Added Successfully",
            result['message'],
            CONFIG['SUCCESS_COLOR'],
            fields=[
                ("📦 Product", product, True),
                ("➕ Added", str(result['added']), True),
                ("🔄 Duplicates", str(result['duplicates']), True),
                # Use the variable here, not the function call
                ("📊 New Stock", str(new_stock), True)
            ]
        )
        await interaction.followup.send(embed=embed, ephemeral=True)
        await log_to_channel(interaction.guild, f"➕ {interaction.user} added {result['added']} keys to **{product}**", CHANNELS['LOG'])
    else:
        await interaction.followup.send(f"❌ Error: {result['message']}", ephemeral=True)

# --- CUSTOMER MANAGEMENT ---
@bot.tree.command(name="customer_dashboard", description="Advanced customer management dashboard")
@app_commands.describe(user="Customer to analyze")
@app_commands.checks.has_permissions(administrator=True)
async def customer_dashboard(interaction: discord.Interaction, user: discord.Member):
    await interaction.response.defer(ephemeral=True)
    
    purchase_data = await key_manager.get_user_purchases_detailed(user.id)
    tier_name, tier_color, tier_benefits = get_customer_tier_advanced(
        purchase_data['lifetime_spent'], 
        purchase_data['total_purchases']
    )
    
    # Calculate additional metrics
    avg_order_value = purchase_data['lifetime_spent'] / max(purchase_data['total_purchases'], 1)
    
    embed = create_embed(
        f"👤 Customer Dashboard: {user.display_name}",
        f"**Current Tier:** {tier_name}\n**Benefits:** {tier_benefits}",
        tier_color,
        thumbnail=user.display_avatar.url,
        fields=[
            ("📊 Purchase Statistics", 
             f"**Total Orders:** {purchase_data['total_purchases']}\n"
             f"**Lifetime Spent:** ${purchase_data['lifetime_spent']:.2f}\n"
             f"**Average Order:** ${avg_order_value:.2f}", True),
            ("👤 Account Info",
             f"**User ID:** `{user.id}`\n"
             f"**Joined:** <t:{int(user.joined_at.timestamp())}:R>\n"
             f"**Status:** {user.status.name.title()}", True),
            ("📈 Next Tier Progress",
             f"**Current:** ${purchase_data['lifetime_spent']:.2f}\n"
             f"**Next Goal:** ${get_next_tier_amount(purchase_data['lifetime_spent']):.2f}", True)
        ]
    )
    
    if purchase_data['purchases']:
        recent_purchases = []
        for purchase in purchase_data['purchases'][:3]:
            recent_purchases.append(f"• **{purchase['product_name']}** - {purchase['count']}x")
        
        embed.add_field(
            name="🛒 Recent Purchases",
            value="\n".join(recent_purchases),
            inline=False
        )
    
    # Add role management buttons
    class CustomerManagementView(discord.ui.View):
        def __init__(self, target_user, tier_info):
            super().__init__(timeout=300)
            self.target_user = target_user
            self.tier_name, self.tier_color, self.tier_benefits = tier_info
        
        @discord.ui.button(label="🏷️ Update Tier Role", style=discord.ButtonStyle.primary)
        async def update_tier_role(self, btn_interaction: discord.Interaction, button: discord.ui.Button):
            # Role update logic here
            role_name = self.tier_name.split(" ", 1)[1] if " " in self.tier_name else self.tier_name
            role = discord.utils.get(btn_interaction.guild.roles, name=role_name)
            
            if role:
                try:
                    await self.target_user.add_roles(role, reason="Customer tier update")
                    await btn_interaction.response.send_message(f"✅ Updated {self.target_user.mention} to {role.mention}", ephemeral=True)
                except discord.Forbidden:
                    await btn_interaction.response.send_message(f"❌ I don't have permission to assign the `{role_name}` role.", ephemeral=True)
            else:
                await btn_interaction.response.send_message(f"❌ Role `{role_name}` not found. Please create it first.", ephemeral=True)
    
    await interaction.followup.send(
        embed=embed, 
        view=CustomerManagementView(user, (tier_name, tier_color, tier_benefits)), 
        ephemeral=True
    )

# --- GIVEAWAY COMMANDS ---
@bot.tree.command(name="giveaway", description="Start a new giveaway")
@app_commands.describe(
    duration="Duration (e.g., 1h, 2d, 1w)",
    winners="Number of winners",
    prize="Prize description"
)
@app_commands.checks.has_permissions(manage_guild=True)
async def giveaway(interaction: discord.Interaction, duration: str, winners: int, prize: str):
    delta = parse_duration(duration)
    if not delta:
        await interaction.response.send_message("❌ Invalid duration format. Use formats like `1h`, `2d`, `1w`", ephemeral=True)
        return
    
    if winners < 1 or winners > 20:
        await interaction.response.send_message("❌ Winner count must be between 1-20", ephemeral=True)
        return

    end_time = datetime.now(timezone.utc) + delta
    giveaway_id = f"{interaction.guild.id}-{int(end_time.timestamp())}"
    
    embed = create_embed(
        "🎁 GIVEAWAY STARTED! 🎁",
        f"**Prize:** {prize}\n**Winners:** {winners}\n**Ends:** <t:{int(end_time.timestamp())}:R>",
        CONFIG['SUCCESS_COLOR'],
        fields=[
            ("🎯 Hosted By", interaction.user.mention, True),
            ("⏰ Ends At", f"<t:{int(end_time.timestamp())}:F>", True),
            # Button will update entry count
        ]
    )
    
    giveaway_channel = await find_or_create_channel(interaction.guild, CHANNELS['GIVEAWAY'])
    if not giveaway_channel:
        await interaction.response.send_message("❌ Could not create/find giveaway channel", ephemeral=True)
        return

    view = GiveawayEntryView(giveaway_id)
    await interaction.response.send_message(f"✅ Giveaway starting in {giveaway_channel.mention}...", ephemeral=True)
    message = await giveaway_channel.send(embed=embed, view=view)
    
    # Store giveaway data
    guild_id_str = str(interaction.guild.id)
    if guild_id_str not in data_manager.data['giveaways']:
        data_manager.data['giveaways'][guild_id_str] = {}
        
    data_manager.data['giveaways'][guild_id_str][str(message.id)] = {
        "channel_id": giveaway_channel.id,
        "message_id": message.id,
        "prize": prize,
        "winner_count": winners,
        "end_time": end_time.isoformat(),
        "entries": [],
        "host": interaction.user.id,
        "giveaway_id": giveaway_id
    }
    data_manager.save_category_data('giveaways')
    
    await log_to_channel(interaction.guild, f"🎁 {interaction.user} started giveaway: **{prize}** ({winners} winners, {duration})", CHANNELS['GIVEAWAY_LOGS'])

@bot.tree.command(name="end_giveaway", description="End a giveaway early")
@app_commands.describe(message_id="Giveaway message ID")
@app_commands.checks.has_permissions(manage_guild=True)
async def end_giveaway(interaction: discord.Interaction, message_id: str):
    guild_id_str = str(interaction.guild.id)
    
    if (guild_id_str not in data_manager.data['giveaways'] or 
        message_id not in data_manager.data['giveaways'][guild_id_str]):
        await interaction.response.send_message("❌ Giveaway not found", ephemeral=True)
        return
    
    await interaction.response.defer(ephemeral=True)
    giveaway_info = data_manager.data['giveaways'][guild_id_str][message_id]
    await end_giveaway_logic(interaction.guild, giveaway_info)
    await interaction.followup.send("✅ Giveaway ended successfully", ephemeral=True)

# Note: Reroll command will not work on giveaways that have been cleaned from the database.
# For a more permanent reroll system, giveaways would need to be archived instead of deleted.
@bot.tree.command(name="reroll_giveaway", description="Reroll winners for an ended giveaway")
@app_commands.describe(message_id="Giveaway message ID", new_winner_count="Number of new winners")
@app_commands.checks.has_permissions(manage_guild=True)
async def reroll_giveaway(interaction: discord.Interaction, message_id: str, new_winner_count: int = 1):
    await interaction.response.defer(ephemeral=True)

    try:
        giveaway_channel = await find_or_create_channel(interaction.guild, CHANNELS['GIVEAWAY'])
        if not giveaway_channel:
            await interaction.followup.send("❌ Giveaway channel not found.", ephemeral=True)
            return

        target_message = await giveaway_channel.fetch_message(int(message_id))
        if not target_message or not target_message.embeds:
            await interaction.followup.send("❌ Could not find a valid giveaway message with that ID.", ephemeral=True)
            return

        # Parse the ended giveaway embed for info
        embed = target_message.embeds[0]
        prize = embed.description.split("**Prize:** ")[1].split("\n")[0] if "**Prize:** " in embed.description else "Unknown Prize"

        # Get entries from reactions (fallback, as original entries are deleted)
        reaction = discord.utils.get(target_message.reactions, emoji="🎁")
        entries = [user async for user in reaction.users() if not user.bot] if reaction else []
        if not entries:
            await interaction.followup.send("❌ No entries found for reroll (no reactions).", ephemeral=True)
            return

        # Select new winners
        winners = random.sample(entries, min(new_winner_count, len(entries)))
        winner_mentions = [f"🏆 {user.mention}" for user in winners]

        # Announce
        reroll_embed = create_embed(
            "🔄 Giveaway Rerolled!",
            f"**Prize:** {prize}\n\n**New Winners:**\n" + "\n".join(winner_mentions),
            CONFIG['SUCCESS_COLOR'],
            fields=[("🏆 Winners Selected", str(len(winners)), True)]
        )
        await giveaway_channel.send(embed=reroll_embed)
        await interaction.followup.send(f"✅ Rerolled giveaway with {len(winners)} new winners!", ephemeral=True)

    except (discord.NotFound, ValueError, IndexError) as e:
        await interaction.followup.send(f"❌ Error during reroll: {str(e)}", ephemeral=True)

# --- MODERATION COMMANDS ---
@bot.tree.command(name="warn", description="Warn a user")
@app_commands.describe(user="User to warn", reason="Warning reason")
@app_commands.checks.has_permissions(kick_members=True)
async def warn(interaction: discord.Interaction, user: discord.Member, reason: str):
    guild_id = str(interaction.guild.id)
    user_id = str(user.id)
    
    if guild_id not in data_manager.data['warnings']:
        data_manager.data['warnings'][guild_id] = {}
    if user_id not in data_manager.data['warnings'][guild_id]:
        data_manager.data['warnings'][guild_id][user_id] = []
    
    warning_data = {
        "reason": reason,
        "moderator": str(interaction.user),
        "moderator_id": interaction.user.id,
        "timestamp": datetime.now(timezone.utc).isoformat()
    }
    
    data_manager.data['warnings'][guild_id][user_id].append(warning_data)
    data_manager.save_category_data('warnings')
    
    warning_count = len(data_manager.data['warnings'][guild_id][user_id])
    
    embed = create_embed(
        "⚠️ User Warned",
        f"**User:** {user.mention}\n**Reason:** {reason}\n**Moderator:** {interaction.user.mention}",
        CONFIG['WARNING_COLOR'],
        thumbnail=user.display_avatar.url,
        fields=[
            ("🔢 Total Warnings", str(warning_count), True),
            ("⏰ Warning Time", f"<t:{int(datetime.now().timestamp())}:F>", True)
        ]
    )
    
    await interaction.response.send_message(embed=embed)
    await log_to_channel(interaction.guild, f"⚠️ {interaction.user} warned {user} | Reason: {reason} | Total warnings: {warning_count}", CHANNELS['MOD_LOG'])

@bot.tree.command(name="warnings", description="View user's warnings")
@app_commands.describe(user="User to check warnings for")
@app_commands.checks.has_permissions(kick_members=True)
async def warnings(interaction: discord.Interaction, user: discord.Member):
    guild_id = str(interaction.guild.id)
    user_id = str(user.id)
    
    if (guild_id not in data_manager.data['warnings'] or 
        user_id not in data_manager.data['warnings'][guild_id] or 
        not data_manager.data['warnings'][guild_id][user_id]):
        await interaction.response.send_message("✅ This user has no warnings", ephemeral=True)
        return
        
    warnings_list = data_manager.data['warnings'][guild_id][user_id]
    embed = create_embed(
        f"⚠️ Warnings for {user.display_name}",
        f"**Total Warnings:** {len(warnings_list)}",
        CONFIG['WARNING_COLOR'],
        thumbnail=user.display_avatar.url
    )
    
    for i, warning in enumerate(warnings_list[-10:], 1):
        timestamp = datetime.fromisoformat(warning['timestamp'])
        embed.add_field(
            name=f"Warning #{len(warnings_list) - 10 + i}",
            value=f"**Reason:** {warning['reason']}\n**By:** {warning['moderator']}\n**Date:** <t:{int(timestamp.timestamp())}:R>",
            inline=False
        )
        
    await interaction.response.send_message(embed=embed, ephemeral=True)

@bot.tree.command(name="clear_warnings", description="Clear all warnings for a user")
@app_commands.describe(user="User to clear warnings for")
@app_commands.checks.has_permissions(kick_members=True)
async def clear_warnings(interaction: discord.Interaction, user: discord.Member):
    guild_id = str(interaction.guild.id)
    user_id = str(user.id)
    
    if (guild_id in data_manager.data['warnings'] and 
        user_id in data_manager.data['warnings'][guild_id] and
        data_manager.data['warnings'][guild_id][user_id]):
        
        warning_count = len(data_manager.data['warnings'][guild_id][user_id])
        data_manager.data['warnings'][guild_id][user_id] = []
        data_manager.save_category_data('warnings')
        
        embed = create_embed(
            "✅ Warnings Cleared",
            f"Cleared **{warning_count}** warnings for {user.mention}",
            CONFIG['SUCCESS_COLOR']
        )
        await interaction.response.send_message(embed=embed, ephemeral=True)
        await log_to_channel(interaction.guild, f"🧹 {interaction.user} cleared {warning_count} warnings for {user}", CHANNELS['MOD_LOG'])
    else:
        await interaction.response.send_message("❌ This user has no warnings to clear", ephemeral=True)

@bot.tree.command(name="tempban", description="Temporarily ban a user")
@app_commands.describe(user="User to tempban", duration="Duration (e.g., 1h, 1d)", reason="Ban reason")
@app_commands.checks.has_permissions(ban_members=True)
async def tempban(interaction: discord.Interaction, user: discord.Member, duration: str, reason: str):
    delta = parse_duration(duration)
    if not delta:
        await interaction.response.send_message("❌ Invalid duration format", ephemeral=True)
        return
        
    end_time = datetime.now(timezone.utc) + delta
    ban_reason = f"Tempban until: {end_time.isoformat()} | Reason: {reason} | Moderator: {interaction.user}"
    
    try:
        await user.ban(reason=ban_reason)
        
        embed = create_embed(
            "🔨 Tempban Issued",
            f"**User:** {user.mention}\n**Duration:** {duration}\n**Reason:** {reason}",
            CONFIG['ERROR_COLOR'],
            thumbnail=user.display_avatar.url,
            fields=[
                ("⏰ Expires", f"<t:{int(end_time.timestamp())}:F>", True),
                ("👮 Moderator", interaction.user.mention, True)
            ]
        )
        
        await interaction.response.send_message(embed=embed)
        await log_to_channel(interaction.guild, f"🔨 {interaction.user} tempbanned {user} for {duration} | Reason: {reason}", CHANNELS['MOD_LOG'])
        
    except discord.Forbidden:
        await interaction.response.send_message("❌ Missing permissions to ban this user", ephemeral=True)

@bot.tree.command(name="mute", description="Timeout a user")
@app_commands.describe(user="User to timeout", duration="Duration (e.g., 10m, 2h)", reason="Timeout reason")
@app_commands.checks.has_permissions(moderate_members=True)
async def mute(interaction: discord.Interaction, user: discord.Member, duration: str, reason: str):
    delta = parse_duration(duration)
    if not delta:
        await interaction.response.send_message("❌ Invalid duration format", ephemeral=True)
        return
        
    try:
        await user.timeout(delta, reason=reason)
        
        embed = create_embed(
            "🔇 User Timed Out",
            f"**User:** {user.mention}\n**Duration:** {duration}\n**Reason:** {reason}",
            CONFIG['WARNING_COLOR'],
            thumbnail=user.display_avatar.url,
            fields=[("👮 Moderator", interaction.user.mention, True)]
        )
        await interaction.response.send_message(embed=embed)
        await log_to_channel(interaction.guild, f"🔇 {interaction.user} muted {user} for {duration} | Reason: {reason}", CHANNELS['MOD_LOG'])
        
    except discord.Forbidden:
        await interaction.response.send_message("❌ Cannot timeout this user (check role hierarchy)", ephemeral=True)

@bot.tree.command(name="unmute", description="Remove user timeout")
@app_commands.describe(user="User to unmute")
@app_commands.checks.has_permissions(moderate_members=True)
async def unmute(interaction: discord.Interaction, user: discord.Member):
    if not user.is_timed_out():
        await interaction.response.send_message("❌ This user is not currently timed out", ephemeral=True)
        return
        
    await user.timeout(None, reason=f"Timeout removed by {interaction.user}")
    await interaction.response.send_message(f"✅ Timeout removed for {user.mention}", ephemeral=True)

@bot.tree.command(name="delete_channels", description="Delete multiple channels and categories")
@app_commands.describe()
async def delete_channels(interaction: discord.Interaction):
    # Check if user has admin permissions
    if not interaction.user.guild_permissions.administrator:
        embed = create_embed(
            "❌ Insufficient Permissions",
            "You need administrator permissions to use this command.",
            CONFIG['ERROR_COLOR']
        )
        await interaction.response.send_message(embed=embed, ephemeral=True)
        return

    view = ChannelManagementView(interaction.guild)
    embed = view.create_management_embed()
    
    await interaction.response.send_message(embed=embed, view=view, ephemeral=True)

# --- STATISTICS & DASHBOARD ---
@bot.tree.command(name="server_stats", description="View comprehensive server statistics")
@app_commands.checks.has_permissions(manage_guild=True)
async def server_stats(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    
    guild = interaction.guild
    guild_id = str(guild.id)
    
    # Basic stats
    total_channels = len(guild.channels)
    text_channels = len(guild.text_channels)
    voice_channels = len(guild.voice_channels)
    categories = len(guild.categories)
    
    # Member stats
    total_members = guild.member_count
    bots = sum(1 for m in guild.members if m.bot)
    humans = total_members - bots
    
    # Ticket stats
    open_tickets = sum(1 for channel in guild.text_channels if channel.name.startswith('ticket-'))
    
    # Vouch stats
    total_vouches = sum(data.get('count', 0) for user_id, data in data_manager.data.get('vouches', {}).items())

    # Giveaway stats
    active_giveaways = len(data_manager.data['giveaways'].get(guild_id, {}))
    
    embed = create_embed(
        f"📊 Server Statistics: {guild.name}",
        f"Comprehensive overview of {guild.name}",
        CONFIG['MAIN_COLOR'],
        thumbnail=guild.icon.url if guild.icon else None,
        fields=[
            ("👥 Members", f"**{total_members}** total\n🤖 {bots} bots\n👤 {humans} humans", True),
            ("📱 Channels", f"**{total_channels}** total\n💬 {text_channels} text\n🔊 {voice_channels} voice\n📂 {categories} categories", True),
            ("🎫 Support", f"**{open_tickets}** open tickets", True),
            ("⭐ Community", f"**{total_vouches}** total vouches", True),
            ("🎁 Events", f"**{active_giveaways}** active giveaways", True),
            ("📅 Server Age", f"<t:{int(guild.created_at.timestamp())}:R>", True)
        ]
    )
    await interaction.followup.send(embed=embed, ephemeral=True)

@bot.tree.command(name="moderation_panel", description="Open the comprehensive moderation dashboard")
@app_commands.checks.has_permissions(administrator=True)
async def moderation_panel(interaction: discord.Interaction):
    guild = interaction.guild
    guild_id = str(guild.id)

    # Calculate moderation stats
    total_warnings = sum(len(warnings) for warnings in data_manager.data['warnings'].get(guild_id, {}).values())
    open_tickets = sum(1 for channel in guild.text_channels if channel.name.startswith('ticket-'))
    
    embed = create_embed(
        "🛡️ Moderation Dashboard",
        f"**Server:** {guild.name}\n**Moderator:** {interaction.user.mention}",
        CONFIG['MAIN_COLOR'],
        thumbnail=guild.icon.url if guild.icon else None,
        fields=[
            ("⚠️ Active Warnings", str(total_warnings), True),
            ("🎫 Open Tickets", str(open_tickets), True),
            ("👥 Total Members", str(guild.member_count), True),
            ("📊 Quick Actions", "Use buttons below", False)
        ]
    )
    
    class ModerationDashboardView(discord.ui.View):
        def __init__(self):
            super().__init__(timeout=300)
        
        @discord.ui.button(label="📊 Detailed Stats", style=discord.ButtonStyle.primary)
        async def detailed_stats(self, btn_interaction: discord.Interaction, button: discord.ui.Button):
            # Get product stock properly
            all_stock = await key_manager.get_product_stock()
            product_count = len(all_stock) if isinstance(all_stock, dict) else 0
            total_stock = sum(all_stock.values()) if isinstance(all_stock, dict) else 0
            
            # Count tickets by category
            ticket_categories = {"purchase": 0, "support": 0, "exchange": 0, "reseller": 0, "media": 0, "giveaway": 0, "report": 0}
            for channel in guild.text_channels:
                if channel.topic and "Reason:" in channel.topic:
                    try:
                        reason = channel.topic.split("Reason: ")[1].split()[0].lower()
                        if reason in ticket_categories:
                            ticket_categories[reason] += 1
                    except (IndexError, AttributeError):
                        pass
            
            stats_embed = create_embed(
                "📈 Detailed Server Analytics",
                "Comprehensive server overview",
                CONFIG['MAIN_COLOR'],
                fields=[
                    ("🎫 Tickets by Category", 
                     f"💳 Purchase: {ticket_categories['purchase']}\n"
                     f"💬 Support: {ticket_categories['support']}\n"
                     f"🔄 Exchange: {ticket_categories['exchange']}", True),
                    ("💰 Key System", 
                     f"Products: {product_count}\n"
                     f"Total Stock: {total_stock}", True),
                    ("🎁 Giveaways", 
                     f"Active: {len(data_manager.data['giveaways'].get(guild_id, {}))}", True)
                ]
            )
            await btn_interaction.response.send_message(embed=stats_embed, ephemeral=True)

        @discord.ui.button(label="💰 Sales Dashboard", style=discord.ButtonStyle.success)
        async def sales_dashboard(self, btn_interaction: discord.Interaction, button: discord.ui.Button):
            guild_id = str(btn_interaction.guild.id)
            stats = await calculate_invoice_stats(guild_id)
            
            # Create a mini version of the invoice dashboard
            embed = create_embed(
                "💰 Sales Overview",
                f"Quick summary of your sales data",
                CONFIG['SUCCESS_COLOR'],
                fields=[
                    ("📈 Total Invoices", f"{stats['total_invoices']:,}", True),
                    ("💰 Total Revenue", f"${stats['total_revenue']:,.2f}", True),
                    ("📅 This Month", f"${stats['revenue_this_month']:,.2f}", True)
                ]
            )
            
            # Create a "View Full Dashboard" button
            class ViewFullDashboardView(discord.ui.View):
                def __init__(self):
                    super().__init__(timeout=30)
                    
                @discord.ui.button(label="View Full Dashboard", style=discord.ButtonStyle.primary)
                async def view_full(self, view_interaction: discord.Interaction, view_button: discord.ui.Button):
                    # Instead of calling invoice_dashboard directly, send a message
                    await view_interaction.response.send_message(
                        "Use `/invoice_dashboard` to view the full dashboard.", 
                        ephemeral=True
                    )
            
            await btn_interaction.response.send_message(embed=embed, view=ViewFullDashboardView(), ephemeral=True)

        @discord.ui.button(label="🧹 Cleanup Tools", style=discord.ButtonStyle.secondary)
        async def cleanup_tools(self, btn_interaction: discord.Interaction, button: discord.ui.Button):
            cleanup_embed = create_embed(
                "🧹 Server Cleanup Tools",
                "Administrative cleanup options",
                CONFIG['MAIN_COLOR'],
                fields=[
                    ("🗑️ Available Actions", 
                     "• `/clear_messages` - Bulk delete messages\n"
                     "• `/clear_warnings` - Clear user warnings\n"
                     "• `/delete_channels` - Delete channels/categories", False),
                    ("⚠️ Warning", "Use cleanup commands carefully!", False)
                ]
            )
            await btn_interaction.response.send_message(embed=cleanup_embed, ephemeral=True)
    
    # Send the message with the view
    await interaction.response.send_message(embed=embed, view=ModerationDashboardView())

# Add this button to your ModerationDashboardView class
@discord.ui.button(label="👥 Delete Roles", style=discord.ButtonStyle.danger)
async def delete_roles_button(self, btn_interaction: discord.Interaction, button: discord.ui.Button):
    delete_roles_embed = create_embed(
        "👥 Role Deletion Tools",
        "⚠️ **WARNING: This will delete ALL roles in the server!**",
        CONFIG['ERROR_COLOR'],
        fields=[
            ("🗑️ Available Action", 
             "• `/delete_all_roles` - Delete all server roles", False),
            ("⚠️ Critical Warning", 
             "This action is **IRREVERSIBLE** and will remove all roles!\n"
             "Make sure you have backups of important role configurations.", False)
        ]
    )
    await btn_interaction.response.send_message(embed=delete_roles_embed, ephemeral=True)


# Simple one-click role deletion command
@bot.tree.command(name="delete_all_roles", description="🗑️ Delete ALL roles in the server")
@app_commands.describe(confirm="Type 'DELETE ALL ROLES' to confirm this dangerous action")
async def delete_all_roles(interaction: discord.Interaction, confirm: str = None):
    # Permission check
    if not interaction.user.guild_permissions.administrator:
        await interaction.response.send_message(
            "❌ You need **Administrator** permission to use this command.", 
            ephemeral=True
        )
        return
    
    guild = interaction.guild
    
    # Get all deletable roles (excluding @everyone and roles above user's highest role)
    deletable_roles = [
        role for role in guild.roles 
        if role != guild.default_role and role < interaction.user.top_role
    ]
    
    # If no confirmation, show preview
    if confirm != "DELETE ALL ROLES":
        preview_embed = create_embed(
            "⚠️ DELETE ALL ROLES - CONFIRMATION REQUIRED",
            f"**This will delete {len(deletable_roles)} roles from {guild.name}!**",
            CONFIG['ERROR_COLOR'],
            fields=[
                ("🔥 To Proceed", 
                 "Run: `/delete_all_roles confirm:DELETE ALL ROLES`", False),
                ("⚠️ WARNING", 
                 "• This action cannot be undone!\n"
                 "• All role permissions will be lost!\n"
                 "• Members will lose their roles!", False),
                ("📊 Roles to Delete", f"{len(deletable_roles)} roles", True)
            ]
        )
        
        # Create a quick dashboard view for the preview
        class RoleDeletionDashboard(discord.ui.View):
            def __init__(self):
                super().__init__(timeout=60)
            
            @discord.ui.button(label="🗑️ CONFIRM DELETE ALL", style=discord.ButtonStyle.danger)
            async def confirm_delete(self, btn_interaction: discord.Interaction, button: discord.ui.Button):
                if btn_interaction.user != interaction.user:
                    await btn_interaction.response.send_message(
                        "❌ Only the command user can confirm this action.", ephemeral=True
                    )
                    return
                
                # Start the deletion process
                await btn_interaction.response.defer(ephemeral=True)
                
                deleted_count = 0
                failed_count = 0
                
                # Create progress embed
                progress_embed = create_embed(
                    "🗑️ Deleting Roles...",
                    f"Processing {len(deletable_roles)} roles...",
                    CONFIG['WARNING_COLOR']
                )
                await btn_interaction.followup.send(embed=progress_embed, ephemeral=True)
                
                # Delete all roles
                for role in deletable_roles:
                    try:
                        await role.delete(reason=f"Bulk role deletion by {interaction.user}")
                        deleted_count += 1
                        await asyncio.sleep(0.3)  # Rate limit protection
                    except Exception:
                        failed_count += 1
                
                # Final result
                result_embed = create_embed(
                    "🗑️ Role Deletion Complete!",
                    f"Deletion process finished for **{guild.name}**",
                    CONFIG['SUCCESS_COLOR'],
                    fields=[
                        ("✅ Deleted", f"{deleted_count} roles", True),
                        ("❌ Failed", f"{failed_count} roles", True),
                        ("📊 Total", f"{len(deletable_roles)} processed", True)
                    ]
                )
                
                await btn_interaction.edit_original_response(embed=result_embed, view=None)
                
                # Log the action
                await log_to_channel(
                    guild,
                    f"🗑️ **BULK ROLE DELETION** by {interaction.user.mention}\n"
                    f"Deleted: {deleted_count} | Failed: {failed_count} | Total: {len(deletable_roles)}",
                    CHANNELS['LOG']
                )
            
            @discord.ui.button(label="❌ Cancel", style=discord.ButtonStyle.secondary)
            async def cancel_delete(self, btn_interaction: discord.Interaction, button: discord.ui.Button):
                if btn_interaction.user != interaction.user:
                    await btn_interaction.response.send_message(
                        "❌ Only the command user can cancel this action.", ephemeral=True
                    )
                    return
                
                cancel_embed = create_embed(
                    "❌ Role Deletion Cancelled",
                    "No roles were deleted.",
                    CONFIG['SUCCESS_COLOR']
                )
                await btn_interaction.response.edit_message(embed=cancel_embed, view=None)
        
        await interaction.response.send_message(embed=preview_embed, view=RoleDeletionDashboard(), ephemeral=True)
        return
    
    # Direct confirmation via command parameter
    await interaction.response.defer(ephemeral=True)
    
    deleted_count = 0
    failed_count = 0
    
    # Progress message
    progress_embed = create_embed(
        "🗑️ Deleting All Roles...",
        f"Processing {len(deletable_roles)} roles in **{guild.name}**...",
        CONFIG['WARNING_COLOR']
    )
    await interaction.followup.send(embed=progress_embed, ephemeral=True)
    
    # Delete all roles with rate limiting
    for role in deletable_roles:
        try:
            await role.delete(reason=f"Bulk role deletion by {interaction.user}")
            deleted_count += 1
            await asyncio.sleep(0.3)  # Prevent rate limits
        except Exception:
            failed_count += 1
    
    # Final results
    result_embed = create_embed(
        "🗑️ Role Deletion Complete!",
        f"All roles deleted from **{guild.name}**",
        CONFIG['SUCCESS_COLOR'],
        fields=[
            ("✅ Successfully Deleted", f"{deleted_count} roles", True),
            ("❌ Failed to Delete", f"{failed_count} roles", True),
            ("📊 Total Processed", f"{len(deletable_roles)} roles", True)
        ]
    )
    
    await interaction.edit_original_response(embed=result_embed)
    
    # Log the action
    await log_to_channel(
        guild,
        f"🗑️ **BULK ROLE DELETION COMPLETE** by {interaction.user.mention}\n"
        f"Deleted: {deleted_count} | Failed: {failed_count} | Total: {len(deletable_roles)}",
        CHANNELS['LOG']
    )
                    
                
@bot.tree.command(name="setup_stats_channels", description="Setup automatic member/bot count channels")
@app_commands.describe(
    member_channel="Voice channel for member count",
    bot_channel="Voice channel for bot count"
)
@app_commands.checks.has_permissions(manage_channels=True)
async def setup_stats_channels(interaction: discord.Interaction, member_channel: discord.VoiceChannel, bot_channel: discord.VoiceChannel):
    guild_id = str(interaction.guild.id)
    
    data_manager.data['stats_channels'][guild_id] = {
        'member_channel': member_channel.id,
        'bot_channel': bot_channel.id
    }
    data_manager.save_category_data('stats_channels')
    
    # Update immediately
    try:
        humans = interaction.guild.member_count - sum(1 for m in interaction.guild.members if m.bot)
        bots = sum(1 for m in interaction.guild.members if m.bot)
        
        await member_channel.edit(name=f"👥 Members: {humans}")
        await bot_channel.edit(name=f"🤖 Bots: {bots}")
        
        embed = create_embed(
            "✅ Stats Channels Configured",
            f"**Member Channel:** {member_channel.mention}\n**Bot Channel:** {bot_channel.mention}\n\nThese will auto-update every 15 minutes.",
            CONFIG['SUCCESS_COLOR']
        )
        await interaction.response.send_message(embed=embed, ephemeral=True)
        
    except discord.Forbidden:
        await interaction.response.send_message("❌ Missing permissions to edit voice channels", ephemeral=True)

# --- AUTO ROLES ---
@bot.tree.command(name="add_auto_role", description="Add a role that's automatically assigned to new members")
@app_commands.describe(role="Role to auto-assign")
@app_commands.checks.has_permissions(manage_roles=True)
async def add_auto_role(interaction: discord.Interaction, role: discord.Role):
    if role >= interaction.guild.me.top_role:
        await interaction.response.send_message("❌ Cannot manage roles higher than my top role", ephemeral=True)
        return
    
    guild_id = str(interaction.guild.id)
    if guild_id not in data_manager.data['auto_roles']:
        data_manager.data['auto_roles'][guild_id] = []
    
    if role.id in data_manager.data['auto_roles'][guild_id]:
        await interaction.response.send_message("❌ This role is already an auto-role", ephemeral=True)
        return
        
    data_manager.data['auto_roles'][guild_id].append(role.id)
    data_manager.save_category_data('auto_roles')
    
    embed = create_embed(
        "✅ Auto-Role Added",
        f"**{role.name}** will now be automatically assigned to new members",
        CONFIG['SUCCESS_COLOR']
    )
    await interaction.response.send_message(embed=embed, ephemeral=True)

@bot.tree.command(name="remove_auto_role", description="Remove a role from auto-assignment")
@app_commands.describe(role="Role to remove from auto-assignment")
@app_commands.checks.has_permissions(manage_roles=True)
async def remove_auto_role(interaction: discord.Interaction, role: discord.Role):
    guild_id = str(interaction.guild.id)
    
    if (guild_id in data_manager.data['auto_roles'] and 
        role.id in data_manager.data['auto_roles'][guild_id]):
        data_manager.data['auto_roles'][guild_id].remove(role.id)
        data_manager.save_category_data('auto_roles')
        
        embed = create_embed(
            "✅ Auto-Role Removed",
            f"**{role.name}** removed from auto-assignment",
            CONFIG['SUCCESS_COLOR']
        )
        await interaction.response.send_message(embed=embed, ephemeral=True)
    else:
        await interaction.response.send_message("❌ That role is not currently auto-assigned", ephemeral=True)

@bot.tree.command(name="list_auto_roles", description="List all auto-assigned roles")
@app_commands.checks.has_permissions(manage_roles=True)
async def list_auto_roles(interaction: discord.Interaction):
    guild_id = str(interaction.guild.id)
    
    if guild_id not in data_manager.data['auto_roles'] or not data_manager.data['auto_roles'][guild_id]:
        await interaction.response.send_message("❌ No auto-roles configured", ephemeral=True)
        return
    
    roles = []
    valid_role_ids = []
    for role_id in data_manager.data['auto_roles'][guild_id]:
        role = interaction.guild.get_role(role_id)
        if role:
            roles.append(f"• {role.name} ({role.mention})")
            valid_role_ids.append(role_id)
    
    # Clean up invalid roles
    if len(valid_role_ids) != len(data_manager.data['auto_roles'][guild_id]):
        data_manager.data['auto_roles'][guild_id] = valid_role_ids
        data_manager.save_category_data('auto_roles')
    
    embed = create_embed(
        "🤖 Auto-Assigned Roles",
        "\n".join(roles) if roles else "No valid auto-roles found",
        CONFIG['MAIN_COLOR'],
        fields=[("📊 Total", str(len(roles)), True)]
    )
    
    await interaction.response.send_message(embed=embed, ephemeral=True)

# --- WELCOME SYSTEM ---
@bot.tree.command(name="setup_welcome", description="Configure welcome messages for new members")
@app_commands.describe(
    channel="Welcome channel",
    title="Welcome title (use {user}, {server}, {member_count})",
    message="Welcome message (use {user}, {server}, {member_count}, and \\n for new lines)", 
    color="Embed color (hex)",
    image_url="Welcome banner image"
)
@app_commands.checks.has_permissions(manage_guild=True)
async def setup_welcome(interaction: discord.Interaction, channel: discord.TextChannel, title: str, message: str, color: str = None, image_url: str = None):
    guild_id = str(interaction.guild.id)
    
    message = message.replace('\\n', '\n')
    
    data_manager.data['welcome'][guild_id] = {
        'channel_id': channel.id,
        'title': title,
        'message': message,
        'color': color or CONFIG['MAIN_COLOR'],
        'image_url': image_url,
        'enabled': True
    }
    data_manager.save_category_data('welcome')
    
    # Show preview
    preview_title = title.replace("{user}", interaction.user.display_name).replace("{server}", interaction.guild.name).replace("{member_count}", str(interaction.guild.member_count))
    preview_message = message.replace("{user}", interaction.user.mention).replace("{server}", interaction.guild.name).replace("{member_count}", str(interaction.guild.member_count))
    
    preview_embed = create_embed(
        preview_title,
        preview_message,
        color or CONFIG['MAIN_COLOR'],
        thumbnail=interaction.user.display_avatar.url,
        image=image_url
    )
    
    await interaction.response.send_message(f"✅ Welcome system configured for {channel.mention}! Preview:", embed=preview_embed, ephemeral=True)

@bot.tree.command(name="toggle_welcome", description="Enable or disable welcome messages")
@app_commands.describe(enabled="True to enable, False to disable")
@app_commands.checks.has_permissions(manage_guild=True)
async def toggle_welcome(interaction: discord.Interaction, enabled: bool):
    guild_id = str(interaction.guild.id)
    
    if guild_id not in data_manager.data['welcome']:
        await interaction.response.send_message("❌ Welcome system not configured. Use `/setup_welcome` first.", ephemeral=True)
        return
    
    data_manager.data['welcome'][guild_id]["enabled"] = enabled
    data_manager.save_category_data('welcome')
    
    status = "enabled" if enabled else "disabled"
    embed = create_embed(
        f"✅ Welcome System {status.title()}",
        f"Welcome messages have been {status}",
        CONFIG['SUCCESS_COLOR'] if enabled else CONFIG['WARNING_COLOR']
    )
    await interaction.response.send_message(embed=embed, ephemeral=True)

# --- VOUCH SYSTEM ---
@bot.tree.command(name="request_vouch", description="Admin: Request a vouch from a customer")
@app_commands.describe(customer="Customer to request vouch from", product="Product they purchased")
@app_commands.checks.has_permissions(administrator=True)
async def request_vouch(interaction: discord.Interaction, customer: discord.Member, product: str):
    embed = create_embed(
        "⭐ Vouch Request",
        f"**{customer.mention}** - How was your experience purchasing **{product}**?\n\nPlease leave an honest review to help other customers!",
        CONFIG['SUCCESS_COLOR'],
        thumbnail=customer.display_avatar.url
    )
    
    await interaction.response.send_message(embed=embed, view=CustomerVouchView(customer, product))

# --- PRODUCT TEMPLATES ---
@bot.tree.command(name="create_product_template", description="Create a reusable product embed template")
@app_commands.describe(
    name="Template name",
    title="Product title", 
    description="Product description (use \\n for new lines)",  # Updated description
    price="Product price",
    features="Key features (separate with |)",
    image_url="Product image URL",
    stock_info="Stock information"
)
@app_commands.checks.has_permissions(administrator=True)
async def create_product_template(interaction: discord.Interaction, name: str, title: str, description: str, price: str, features: str, image_url: str = None, stock_info: str = "Contact for availability"):
    
    description = description.replace('\\n', '\n')
    
    feature_list = [f.strip() for f in features.split('|') if f.strip()]
    features_text = "\n".join([f"✓ {feature}" for feature in feature_list])
    
    template_data = {
        'title': title,
        'description': description,  
        'price': price,
        'features': features_text,
        'image_url': image_url,
        'stock_info': stock_info,
        'color': CONFIG['MAIN_COLOR'],
        'created_by': interaction.user.id,
        'created_at': datetime.now().isoformat()
    }
    
    data_manager.data['templates'][name] = template_data
    data_manager.save_category_data('templates')
    
    # Show preview
    preview_embed = create_embed(
        f"🛒 {title}",
        f"{description}\n\n**💰 Price:** {price}\n\n**✨ Features:**\n{features_text}",
        CONFIG['MAIN_COLOR'],
        image=image_url,
        fields=[("📦 Stock", stock_info, True)]
    )
    
    await interaction.response.send_message(f"✅ Product template `{name}` created! Preview:", embed=preview_embed, ephemeral=True)

@bot.tree.command(name="post_product", description="Post a product embed using a template")
@app_commands.describe(template="The name of the template to use.", channel="The channel to post the product in.")
@app_commands.checks.has_permissions(manage_messages=True)
async def post_product(interaction: discord.Interaction, template: str, channel: discord.TextChannel):
    if template not in data_manager.data['templates']:
        available = ", ".join(data_manager.data['templates'].keys()) or "None"
        await interaction.response.send_message(f"❌ Template '{template}' not found. Available: {available}", ephemeral=True)
        return
    
    template_data = data_manager.data['templates'][template]
    
    product_embed = discord.Embed(
        title=f"**{template_data['title']}**",
        description=f"**{template_data.get('short_description', 'Drücke den Button für mehr Infos!')}**",
        color=template_data.get('color', 0x303136)
    )

    # Format features with proper Discord formatting
    if template_data.get('features'):
        features_text = "**Product Features:**\n"
        feature_lines = template_data['features'].split('\n')
        for line in feature_lines:
            clean_line = line.replace('✓', '').replace('•', '').strip()
            if clean_line:
                features_text += f"> **✓** {clean_line}\n"  # Use quote blocks for features

        product_embed.add_field(
            name="\u200b",  # Zero-width space for better formatting
            value=features_text,
            inline=False
        )

# Format pricing with code blocks for better readability
    if template_data.get('price'):
        prices = [p.strip() for p in template_data['price'].split('|')]
        price_text = "**Pricing:**\n"

        for price_info in prices:
            if ':' in price_info:
                duration, cost = price_info.split(':', 1)
                duration = duration.strip()
                cost = cost.strip()

                if "not available" in cost.lower():
                    price_text += f"> ~~`{duration}`~~ - ~~{cost}~~\n"
                else:
                    price_text += f"> `{duration}` - **{cost}**\n"

        product_embed.add_field(
            name="\u200b",
            value=price_text,
            inline=False
        )
    
    # Format PRICING as a field with special styling
    if template_data.get('price'):
        prices = [p.strip() for p in template_data['price'].split('|')]
        
        # Create formatted price blocks
        price_blocks = []
        
        # Group prices by type (RageMP vs Alt:V) if applicable
        rage_prices = []
        altv_prices = []
        general_prices = []
        
        for price_info in prices:
            if ':' in price_info:
                duration, cost = price_info.split(':', 1)
                duration = duration.strip()
                cost = cost.strip()
                
                # Create styled price entry
                if "ragemp" in duration.lower():
                    if "not available" in cost.lower():
                        rage_prices.append(f"` {duration:<20} `  ~~{cost}~~")
                    else:
                        rage_prices.append(f"` {duration:<20} `  **{cost}**")
                elif "alt:v" in duration.lower() or "altv" in duration.lower():
                    if "not available" in cost.lower():
                        altv_prices.append(f"` {duration:<20} `  ~~Not Available~~")
                    else:
                        altv_prices.append(f"` {duration:<20} `  **{cost}**")
                else:
                    # General pricing (like Hydrogen)
                    general_prices.append(f"` {duration:<15} `  **{cost}**")
        
        # Build the price display
        price_display = ""
        
        if rage_prices:
            price_display += "**RageMP:**\n"
            price_display += "\n".join(rage_prices) + "\n\n"
        
        if altv_prices:
            price_display += "**Alt:V:**\n"
            price_display += "\n".join(altv_prices)
        
        if general_prices:
            price_display = "\n".join(general_prices)
        
        product_embed.add_field(
            name="**Pricing**",
            value=price_display.strip(),
            inline=False
        )
    
    # Add stock status as a small field
    if template_data.get('stock_info'):
        product_embed.add_field(
            name="**📦 Stock Status**",
            value=f"`{template_data['stock_info']}`",
            inline=True
        )
    
    # Add purchase info field
    product_embed.add_field(
        name="**🛒 How to Purchase**",
        value="`Click 'More Details' below`",
        inline=True
    )
    
    # Set image if exists (at the bottom like in screenshots)
    if template_data.get('image_url'):
        product_embed.set_image(url=template_data['image_url'])
    
    # Set footer with branding
    branding = get_branding_data()
    product_embed.set_footer(
        text=f"{branding['company_name']} • Premium Service",
        icon_url=branding.get('logo_url')
    )
    
    # Add timestamp
    product_embed.timestamp = datetime.now(timezone.utc)
    
    # Send with interactive view
    await channel.send(embed=product_embed, view=ProductPostView(template_data, interaction.guild.id))
    
    await interaction.response.send_message(f"✅ Product `{template}` posted in {channel.mention}", ephemeral=True)

@bot.tree.command(name="list_templates", description="List all available product templates")
@app_commands.checks.has_permissions(manage_messages=True)
async def list_templates(interaction: discord.Interaction):
    if not data_manager.data['templates']:
        await interaction.response.send_message("❌ No templates found", ephemeral=True)
        return
    
    embed = create_embed(
        "🧩 Available Product Templates", 
        f"Total templates: {len(data_manager.data['templates'])}",
        CONFIG['MAIN_COLOR']
    )
    
    for name, template in data_manager.data['templates'].items():
        embed.add_field(
            name=f"📦 {name}",
            value=f"**Title:** {template.get('title', 'No title')}\n**Price:** {template.get('price', 'No price')}",
            inline=True
        )
    
    await interaction.response.send_message(embed=embed, ephemeral=True)

@post_product.autocomplete('template')
async def post_product_autocomplete(
    interaction: discord.Interaction,
    current: str,
) -> List[app_commands.Choice[str]]:
    templates = data_manager.data.get('templates', {}).keys()
    return [
        app_commands.Choice(name=template, value=template)
        for template in templates if current.lower() in template.lower()
    ][:25]  # Discord limits to 25 choices    

@bot.tree.command(name="find_ticket_panel", description="Automatically find and configure the ticket panel in your server")
@app_commands.checks.has_permissions(administrator=True)
async def find_ticket_panel(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    
    found_panels = []
    
    # Search all text channels for ticket panels
    for channel in interaction.guild.text_channels:
        try:
            # Check if bot can read the channel
            if not channel.permissions_for(interaction.guild.me).read_message_history:
                continue
            
            # Search recent messages for ticket panels
            async for message in channel.history(limit=100):
                if (message.author == bot.user and 
                    message.embeds and 
                    any(view for view in message.components if any(isinstance(component, discord.SelectMenu) for component in view.children))):
                    
                    # Check if it's likely a ticket panel
                    embed = message.embeds[0]
                    if ("ticket" in embed.title.lower() or 
                        "support" in embed.title.lower() or 
                        any("ticket" in str(field.value).lower() for field in embed.fields)):
                        
                        found_panels.append({
                            'channel': channel,
                            'message': message,
                            'embed_title': embed.title,
                            'age': datetime.now(timezone.utc) - message.created_at
                        })
        except discord.Forbidden:
            continue
        except Exception as e:
            logger.error(f"Error searching channel {channel.name}: {e}")
    
    if not found_panels:
        # No panels found, offer to create one
        embed = create_embed(
            "❌ No Ticket Panels Found",
            "I couldn't find any existing ticket panels in your server.",
            CONFIG['WARNING_COLOR'],
            fields=[
                ("🔧 Solution", "Use `/ticket` to create a new ticket panel", False),
                ("📍 Recommended", "Create it in #create-ticket channel", False)
            ]
        )
        
        class CreatePanelView(discord.ui.View):
            def __init__(self):
                super().__init__(timeout=60)
            
            @discord.ui.button(label="🎫 Create Ticket Panel Now", style=discord.ButtonStyle.success)
            async def create_panel(self, btn_interaction: discord.Interaction, button: discord.ui.Button):
                # Find or suggest a channel
                ticket_channel = discord.utils.get(interaction.guild.text_channels, name="create-ticket")
                if ticket_channel:
                    await btn_interaction.response.send_message(
                        f"Please use `/ticket` in {ticket_channel.mention} to create the panel.", 
                        ephemeral=True
                    )
                else:
                    await btn_interaction.response.send_message(
                        "Please create a #create-ticket channel first, then use `/ticket` in that channel.", 
                        ephemeral=True
                    )
        
        await interaction.followup.send(embed=embed, view=CreatePanelView(), ephemeral=True)
        return
    
    # Sort by most recent
    found_panels.sort(key=lambda x: x['age'])
    
    embed = create_embed(
        f"🔍 Found {len(found_panels)} Ticket Panel(s)",
        "Select which ticket panel to use for product redirects:",
        CONFIG['SUCCESS_COLOR']
    )
    
    class PanelSelectionView(discord.ui.View):
        def __init__(self, panels):
            super().__init__(timeout=300)
            self.panels = panels
            
            # Create dropdown with found panels
            options = []
            for i, panel in enumerate(panels[:25]):  # Discord limit
                age_text = f"{panel['age'].days}d ago" if panel['age'].days > 0 else "Today"
                options.append(discord.SelectOption(
                    label=f"#{panel['channel'].name} - {panel['embed_title'][:30]}",
                    value=str(i),
                    description=f"Created {age_text} • Message ID: {panel['message'].id}"
                ))
            
            if options:
                self.add_item(PanelSelect(options, panels))
    
    class PanelSelect(discord.ui.Select):
        def __init__(self, options, panels):
            super().__init__(placeholder="Choose a ticket panel to configure...", options=options)
            self.panels = panels
        
        async def callback(self, select_interaction: discord.Interaction):
            selected_index = int(self.values[0])
            selected_panel = self.panels[selected_index]
            
            # Configure this panel
            guild_id = str(select_interaction.guild.id)
            if guild_id not in data_manager.data['ticket_config']:
                data_manager.data['ticket_config'][guild_id] = {}
            
            data_manager.data['ticket_config'][guild_id]['ticket_channel_id'] = selected_panel['channel'].id
            data_manager.data['ticket_config'][guild_id]['ticket_message_id'] = selected_panel['message'].id
            data_manager.save_category_data('ticket_config')
            
            # Test the URL
            ticket_url = f"https://discord.com/channels/{select_interaction.guild.id}/{selected_panel['channel'].id}/{selected_panel['message'].id}"
            
            success_embed = create_embed(
                "✅ Ticket Panel Configured Successfully!",
                f"Your ticket panel has been configured and is ready to use.",
                CONFIG['SUCCESS_COLOR'],
                fields=[
                    ("📍 Channel", selected_panel['channel'].mention, True),
                    ("📋 Panel Title", selected_panel['embed_title'], True),
                    ("🔗 Direct URL", f"[Click to Test]({ticket_url})", False),
                    ("✅ Status", "Active and Ready", True)
                ]
            )
            
            await select_interaction.response.edit_message(embed=success_embed, view=None)
    
    # Add panels to embed
    for i, panel in enumerate(found_panels[:5]):  # Show first 5
        age_text = f"{panel['age'].days} days ago" if panel['age'].days > 0 else "Today"
        embed.add_field(
            name=f"#{panel['channel'].name}",
            value=f"**Title:** {panel['embed_title']}\n**Created:** {age_text}\n**ID:** `{panel['message'].id}`",
            inline=True
        )
    
    await interaction.followup.send(embed=embed, view=PanelSelectionView(found_panels), ephemeral=True)

# --- UTILITY COMMANDS ---
@bot.tree.command(name="afk", description="Set your AFK status")
@app_commands.describe(reason="AFK reason")
async def afk(interaction: discord.Interaction, reason: str = "I am AFK"):
    user_id = str(interaction.user.id)
    
    if user_id in data_manager.data['afk']:
        await interaction.response.send_message("⚠️ You are already AFK. Status updated.", ephemeral=True)
    else:
        await interaction.response.send_message(f"✅ You are now AFK: `{reason}`", ephemeral=True)
    
    data_manager.data['afk'][user_id] = reason
    data_manager.save_category_data('afk')

@bot.tree.command(name="userinfo", description="Get detailed user information")
@app_commands.describe(user="User to get info for (optional)")
async def userinfo(interaction: discord.Interaction, user: discord.Member = None):
    member = user or interaction.user
    
    # Calculate account and server age
    account_age = datetime.now(timezone.utc) - member.created_at
    server_age = datetime.now(timezone.utc) - member.joined_at if member.joined_at else None
    
    embed = create_embed(
        f"👤 User Information: {member.display_name}",
        f"**Username:** {member.name}#{member.discriminator}\n**Display Name:** {member.display_name}",
        CONFIG['MAIN_COLOR'],
        thumbnail=member.display_avatar.url,
        fields=[
            ("🆔 User ID", f"`{member.id}`", True),
            ("📅 Account Created", f"<t:{int(member.created_at.timestamp())}:F>\n*{account_age.days} days ago*", True),
            ("🔗 Profile", member.mention, True)
        ]
    )
    
    if member.joined_at:
        embed.add_field(
            name="📥 Joined Server", 
            value=f"<t:{int(member.joined_at.timestamp())}:F>\n*{server_age.days} days ago*", 
            inline=True
        )
    
    # Status
    status_emoji = {
        discord.Status.online: "🟢", discord.Status.idle: "🟡",
        discord.Status.dnd: "🔴", discord.Status.offline: "⚫"
    }
    embed.add_field(
        name="📡 Status", 
        value=f"{status_emoji.get(member.status, '❓')} {member.status.name.title()}", 
        inline=True
    )
    
    # Roles
    roles = [role.mention for role in reversed(member.roles) if role.name != "@everyone"]
    if roles:
        roles_text = ", ".join(roles) if len(", ".join(roles)) <= 1024 else f"{len(roles)} roles (too many to display)"
        embed.add_field(name=f"🏷️ Roles ({len(roles)})", value=roles_text, inline=False)
    
    await interaction.response.send_message(embed=embed, ephemeral=True)

@bot.tree.command(name="serverinfo", description="Get detailed server information")
async def serverinfo(interaction: discord.Interaction):
    guild = interaction.guild
    
    # Member status counts
    online = sum(1 for m in guild.members if m.status == discord.Status.online)
    idle = sum(1 for m in guild.members if m.status == discord.Status.idle) 
    dnd = sum(1 for m in guild.members if m.status == discord.Status.dnd)
    offline = guild.member_count - (online + idle + dnd)
    
    embed = create_embed(
        f"🏠 Server Information: {guild.name}",
        guild.description or "No server description set",
        CONFIG['MAIN_COLOR'],
        thumbnail=guild.icon.url if guild.icon else None,
        fields=[
            ("👑 Owner", guild.owner.mention if guild.owner else "Unknown", True),
            ("🆔 Server ID", f"`{guild.id}`", True),
            ("📅 Created", f"<t:{int(guild.created_at.timestamp())}:F>", True),
            ("👥 Members", f"**{guild.member_count}** total\n🟢 {online} | 🟡 {idle} | 🔴 {dnd} | ⚫ {offline}", True),
            ("📱 Channels", f"**{len(guild.channels)}** total\n💬 {len(guild.text_channels)} Text\n🔊 {len(guild.voice_channels)} Voice", True),
            ("🛡️ Security", f"Verification: {str(guild.verification_level).title()}", True)
        ]
    )
    
    if guild.banner:
        embed.set_image(url=guild.banner.url)
    
    await interaction.response.send_message(embed=embed, ephemeral=True)

@bot.tree.command(name="clear_messages", description="Delete messages from a channel")
@app_commands.describe(amount="Number of messages (1-100)", channel="Target channel")
@app_commands.checks.has_permissions(manage_messages=True)
async def clear_messages(interaction: discord.Interaction, amount: int, channel: discord.TextChannel = None):
    if amount < 1 or amount > 100:
        await interaction.response.send_message("❌ Amount must be between 1-100", ephemeral=True)
        return

    target_channel = channel or interaction.channel
    await interaction.response.defer(ephemeral=True)

    try:
        deleted = await target_channel.purge(limit=amount)
        embed = create_embed(
            "✅ Messages Cleared",
            f"Successfully deleted **{len(deleted)}** messages from {target_channel.mention}",
            CONFIG['SUCCESS_COLOR']
        )
        await interaction.followup.send(embed=embed, ephemeral=True)
        await log_to_channel(interaction.guild, f"🧹 {interaction.user} cleared {len(deleted)} messages in {target_channel.mention}", CHANNELS['MOD_LOG'])
        
    except discord.Forbidden:
        await interaction.followup.send("❌ Missing permissions to delete messages", ephemeral=True)

@bot.tree.command(name="poll", description="Create a poll with multiple options")
@app_commands.describe(
    question="Poll question",
    option1="First option", option2="Second option", option3="Third option (optional)",
    option4="Fourth option (optional)", option5="Fifth option (optional)"
)
async def poll(interaction: discord.Interaction, question: str, option1: str, option2: str, 
               option3: str = None, option4: str = None, option5: str = None):
    
    options = [opt for opt in [option1, option2, option3, option4, option5] if opt is not None]
    emojis = ["1️⃣", "2️⃣", "3️⃣", "4️⃣", "5️⃣"]
    
    poll_description = "\n".join([f"{emojis[i]} {opt}" for i, opt in enumerate(options)])
    
    embed = create_embed(
        f"📊 Poll: {question}",
        f"{poll_description}\n\n**How to vote:** React with the corresponding number!",
        CONFIG['MAIN_COLOR'],
        fields=[("📝 Created By", interaction.user.mention, True)]
    )
    
    await interaction.response.send_message(embed=embed)
    message = await interaction.original_response()
    
    for i in range(len(options)):
        try:
            await message.add_reaction(emojis[i])
        except discord.HTTPException:
            pass

@bot.tree.command(name="announce", description="Make a server announcement")
@app_commands.describe(
    title="Announcement title",
    message="Announcement content",
    channel="Channel to post in",
    ping_everyone="Ping @everyone"
)
@app_commands.checks.has_permissions(mention_everyone=True)
async def announce(interaction: discord.Interaction, title: str, message: str, channel: discord.TextChannel, ping_everyone: bool = False):
    embed = create_embed(
        f"📢 {title}",
        message,
        CONFIG['SUCCESS_COLOR'],
        fields=[
            ("📝 Announced By", interaction.user.mention, True),
            ("⏰ Time", f"<t:{int(datetime.now().timestamp())}:F>", True)
        ]
    )
    
    content = "@everyone" if ping_everyone else None
    
    try:
        await channel.send(content=content, embed=embed, allowed_mentions=discord.AllowedMentions(everyone=ping_everyone))
        await interaction.response.send_message(f"✅ Announcement posted in {channel.mention}", ephemeral=True)
    except discord.Forbidden:
        await interaction.response.send_message("❌ Missing permissions to post in that channel", ephemeral=True)

@bot.tree.command(name="payment", description="Display payment methods")
async def payment_menu(interaction: discord.Interaction):
    embed = create_embed(
        "💳 Payment Methods",
        "**Accepted payment options for purchases:**",
        CONFIG['MAIN_COLOR'],
        fields=[
            ("💸 PayPal", "**Email:** northernhub@paypal.com\n**Type:** Friends & Family only\n**Note:** No messages or notes with payment!", False),
            ("🇮🇳 UPI (India)", "**ID:** northernhub@upi\n**Apps:** GPay, PhonePe, Paytm", False),
            ("💳 Paysafecard", "Contact own for PSC instructions", False),
            ("🪙 Cryptocurrency", "**USDT (TRC20):** `TCDS7KCqQ9UAoNLzDmwmCQWc9VKNFZF119`\n**ETH (ERC20):** `0xbcc4e7a15cca51d00ab1beee4bcce299254f592f`\n**LTC:** `LXKbMjHdx7ZgH8kEoF9JsaUiSqErWo7zei`\n**Note:** More available tag Owner for more info!", False),
            ("⚠️ Important", "• Always use F&F for PayPal\n• No refunds on any payment method\n• Contact support for crypto rates", False)
        ]
    )
    
    await interaction.response.send_message(embed=embed)

@bot.tree.command(name="create_tos", description="Post the server's Terms of Service")
@app_commands.describe(channel="Channel to post in", banner_url="Banner image URL")
@app_commands.checks.has_permissions(administrator=True)
async def create_tos(interaction: discord.Interaction, channel: discord.TextChannel = None, banner_url: str = None):
    target_channel = channel or interaction.channel
    
    tos_text = """**By using our services, you agree to the following terms:**

🔒 **1. No Refunds Policy**
All sales are final. No refunds under any circumstances.

💳 **2. Payment Requirements**
• PayPal payments must be sent as Friends & Family
• No notes or messages with payments
• Cryptocurrency payments accepted

⭐ **3. Review & Vouch Policy**
• Accurate product names required in reviews
• Misleading reviews result in permanent bans
• Spam reviews forfeit product access

🛡️ **4. Product Warranty**
• No warranty unless explicitly stated
• Users responsible for account security
• Immediate protection recommended

🚫 **5. Server Rules**
• No scam accusations (instant ban)
• No spam (results in ban + product loss)
• No staff DMs for support (use tickets only)
• Leaving server = product revocation
• Delivery times may vary

**Violation of these terms results in immediate action.**"""
    
    embed = create_embed(
        "📜 Terms of Service",
        tos_text,
        CONFIG['MAIN_COLOR'],
        image=banner_url
    )
    
    try:
        await target_channel.send(embed=embed)
        await interaction.response.send_message(f"✅ Terms of Service posted in {target_channel.mention}", ephemeral=True)
    except discord.Forbidden:
        await interaction.response.send_message("❌ Missing permissions to post in that channel", ephemeral=True)

if __name__ == "__main__":
    try:
        token = os.getenv("DISCORD_TOKEN")
        if not token:
            raise ValueError("DISCORD_TOKEN not found in environment variables.")
        bot.run(token)
    except Exception as e:
        logger.critical(f"CRITICAL: Failed to start bot - {e}")
