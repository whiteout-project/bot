"""
UI Components for Gift Operations Module

This module contains Discord UI components for the gift operations functionality.
"""

import discord
import asyncio
import time
from datetime import datetime

class OCRSettingsView(discord.ui.View):
    """UI View for OCR settings management."""
    
    def __init__(self, gift_operations, ocr_settings, ddddocr_available):
        super().__init__(timeout=300)
        self.gift_operations = gift_operations
        self.ocr_settings = ocr_settings
        self.ddddocr_available = ddddocr_available
        
        # Disable buttons if ddddocr is not available
        if not ddddocr_available:
            for item in self.children:
                if hasattr(item, 'disabled'):
                    item.disabled = True

    @discord.ui.button(label="Toggle OCR", style=discord.ButtonStyle.primary, emoji="🤖")
    async def toggle_ocr(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Toggle OCR enabled/disabled."""
        if not self.ddddocr_available:
            await interaction.response.send_message(
                "❌ OCR cannot be toggled because ddddocr library is not available.",
                ephemeral=True
            )
            return
        
        current_enabled = self.ocr_settings[0]
        new_enabled = 0 if current_enabled == 1 else 1
        
        success, message = await self.gift_operations.update_ocr_settings(
            interaction, enabled=new_enabled
        )
        
        if success:
            self.ocr_settings = (new_enabled, self.ocr_settings[1])
            await interaction.response.send_message(f"✅ {message}", ephemeral=True)
            # Refresh the settings display
            await self.gift_operations.show_ocr_settings(interaction)
        else:
            await interaction.response.send_message(f"❌ {message}", ephemeral=True)

    @discord.ui.button(label="Image Saving", style=discord.ButtonStyle.secondary, emoji="💾")
    async def toggle_save_images(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Cycle through image saving options."""
        current_save_images = self.ocr_settings[1]
        new_save_images = (current_save_images + 1) % 4  # Cycle 0->1->2->3->0
        
        success, message = await self.gift_operations.update_ocr_settings(
            interaction, save_images=new_save_images
        )
        
        if success:
            self.ocr_settings = (self.ocr_settings[0], new_save_images)
            await interaction.response.send_message(f"✅ {message}", ephemeral=True)
            # Refresh the settings display
            await self.gift_operations.show_ocr_settings(interaction)
        else:
            await interaction.response.send_message(f"❌ {message}", ephemeral=True)

    @discord.ui.button(label="Test CAPTCHA", style=discord.ButtonStyle.success, emoji="🧪")
    async def test_captcha(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Test CAPTCHA solving with current settings."""
        if not self.ddddocr_available:
            await interaction.response.send_message(
                "❌ CAPTCHA testing is not available because ddddocr library is missing.",
                ephemeral=True
            )
            return
        
        user_id = interaction.user.id
        current_time = time.time()
        
        # Check cooldown
        if user_id in self.gift_operations.test_captcha_cooldowns:
            time_since_last = current_time - self.gift_operations.test_captcha_cooldowns[user_id]
            if time_since_last < self.gift_operations.test_captcha_delay:
                remaining = self.gift_operations.test_captcha_delay - time_since_last
                await interaction.response.send_message(
                    f"⏳ Please wait {remaining:.0f} seconds before testing again.",
                    ephemeral=True
                )
                return
        
        self.gift_operations.test_captcha_cooldowns[user_id] = current_time
        
        await interaction.response.send_message(
            "🧪 Testing CAPTCHA solving... This may take a moment.",
            ephemeral=True
        )
        
        try:
            test_fid = self.gift_operations.get_test_fid()
            
            # Check if OCR is enabled
            enabled, _ = self.ocr_settings
            if enabled != 1 or not self.gift_operations.captcha_solver:
                await interaction.followup.send(
                    "❌ OCR is disabled or solver not initialized. Cannot test CAPTCHA.",
                    ephemeral=True
                )
                return
            
            # Fetch a test captcha
            captcha_base64, error = await self.gift_operations.fetch_captcha(test_fid)
            
            if error or not captcha_base64:
                await interaction.followup.send(
                    f"❌ Failed to fetch test CAPTCHA: {error or 'Unknown error'}",
                    ephemeral=True
                )
                return
            
            # Decode and solve
            import base64
            if captcha_base64.startswith("data:image"):
                img_b64_data = captcha_base64.split(",", 1)[1]
            else:
                img_b64_data = captcha_base64
            
            image_bytes = base64.b64decode(img_b64_data)
            
            start_time = time.time()
            captcha_code, success, method, confidence, _ = await self.gift_operations.captcha_solver.solve_captcha(
                image_bytes, fid=test_fid, attempt=0
            )
            solve_time = time.time() - start_time
            
            if success:
                result_embed = discord.Embed(
                    title="✅ CAPTCHA Test Successful",
                    color=discord.Color.green()
                )
                result_embed.add_field(
                    name="Results",
                    value=(
                        f"**Solved Code:** `{captcha_code}`\n"
                        f"**Method:** {method}\n"
                        f"**Confidence:** {confidence:.2f}\n"
                        f"**Solve Time:** {solve_time:.2f}s"
                    ),
                    inline=False
                )
            else:
                result_embed = discord.Embed(
                    title="❌ CAPTCHA Test Failed", 
                    color=discord.Color.red()
                )
                result_embed.add_field(
                    name="Results",
                    value=(
                        f"**Status:** OCR failed to solve\n"
                        f"**Method:** {method}\n"
                        f"**Solve Time:** {solve_time:.2f}s"
                    ),
                    inline=False
                )
            
            await interaction.followup.send(embed=result_embed, ephemeral=True)
            
        except Exception as e:
            self.gift_operations.logger.exception(f"Error during CAPTCHA test: {e}")
            await interaction.followup.send(
                f"❌ Error during CAPTCHA test: {str(e)}",
                ephemeral=True
            )

    @discord.ui.button(label="Update Test FID", style=discord.ButtonStyle.secondary, emoji="🆔")
    async def update_test_fid(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Update the test FID."""
        modal = TestFIDModal(self.gift_operations)
        await interaction.response.send_modal(modal)

    @discord.ui.button(label="Reset Stats", style=discord.ButtonStyle.danger, emoji="🔄")
    async def reset_stats(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Reset processing statistics."""
        # Reset OCR processing stats
        self.gift_operations.processing_stats = {
            "ocr_solver_calls": 0,
            "ocr_valid_format": 0,
            "captcha_submissions": 0,
            "server_validation_success": 0,
            "server_validation_failure": 0,
            "total_fids_processed": 0,
            "total_processing_time": 0.0,
            "commercial_solver_calls": 0,
            "commercial_solver_success": 0,
            "commercial_solver_failure": 0
        }
        
        # Reset commercial captcha stats if available
        if self.gift_operations.commercial_captcha_solver:
            self.gift_operations.commercial_captcha_solver.reset_stats()
        
        await interaction.response.send_message(
            "✅ All processing statistics have been reset.",
            ephemeral=True
        )
        
        # Refresh the settings display
        await self.gift_operations.show_ocr_settings(interaction)


class TestFIDModal(discord.ui.Modal):
    """Modal for updating test FID."""
    
    def __init__(self, gift_operations):
        super().__init__(title="Update Test FID")
        self.gift_operations = gift_operations

    fid_input = discord.ui.TextInput(
        label="Test FID",
        placeholder="Enter the new test FID (e.g., 244886619)",
        required=True,
        max_length=20
    )

    async def on_submit(self, interaction: discord.Interaction):
        new_fid = self.fid_input.value.strip()
        
        # Validate FID format
        if not new_fid.isdigit():
            await interaction.response.send_message(
                "❌ FID must contain only numbers.",
                ephemeral=True
            )
            return
        
        # Verify the FID
        await interaction.response.send_message(
            "🔍 Verifying the new test FID...",
            ephemeral=True
        )
        
        try:
            is_valid, message = await self.gift_operations.verify_test_fid(new_fid)
            
            if is_valid:
                success = await self.gift_operations.update_test_fid(new_fid)
                if success:
                    await interaction.followup.send(
                        f"✅ Test FID updated successfully to `{new_fid}`\n"
                        f"Verification: {message}",
                        ephemeral=True
                    )
                else:
                    await interaction.followup.send(
                        "❌ Failed to update test FID in database.",
                        ephemeral=True
                    )
            else:
                await interaction.followup.send(
                    f"❌ FID verification failed: {message}",
                    ephemeral=True
                )
                
        except Exception as e:
            self.gift_operations.logger.exception(f"Error updating test FID: {e}")
            await interaction.followup.send(
                f"❌ Error updating test FID: {str(e)}",
                ephemeral=True
            )


class GiftView(discord.ui.View):
    """Main gift operations view."""
    
    def __init__(self, gift_operations):
        super().__init__(timeout=300)
        self.gift_operations = gift_operations

    @discord.ui.button(label="Create Gift Code", style=discord.ButtonStyle.success, emoji="🎫")
    async def create_gift_code(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.gift_operations.create_gift_code(interaction)

    @discord.ui.button(label="CAPTCHA Settings", style=discord.ButtonStyle.primary, emoji="🔍")
    async def show_ocr_settings(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.gift_operations.show_ocr_settings(interaction)

    @discord.ui.button(label="List Gift Codes", style=discord.ButtonStyle.secondary, emoji="📋")
    async def list_gift_codes(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.gift_operations.list_gift_codes(interaction)

    @discord.ui.button(label="Delete Gift Code", style=discord.ButtonStyle.danger, emoji="❌")
    async def delete_gift_code(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.gift_operations.delete_gift_code(interaction)

    @discord.ui.button(label="Gift Code Channel", style=discord.ButtonStyle.secondary, emoji="📢")
    async def setup_gift_channel(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.gift_operations.setup_gift_channel(interaction)


class CreateGiftCodeModal(discord.ui.Modal):
    """Modal for creating gift codes."""
    
    def __init__(self, gift_operations):
        super().__init__(title="Create Gift Code")
        self.gift_operations = gift_operations

    giftcode_input = discord.ui.TextInput(
        label="Gift Code",
        placeholder="Enter the gift code",
        required=True,
        max_length=50
    )

    async def on_submit(self, interaction: discord.Interaction):
        giftcode = self.giftcode_input.value.strip()
        
        if not giftcode:
            await interaction.response.send_message(
                "❌ Gift code cannot be empty.",
                ephemeral=True
            )
            return
        
        # Clean the gift code
        giftcode = self.gift_operations.clean_gift_code(giftcode)
        
        try:
            # Check if code already exists
            self.gift_operations.cursor.execute("SELECT 1 FROM gift_codes WHERE giftcode = ?", (giftcode,))
            if self.gift_operations.cursor.fetchone():
                await interaction.response.send_message(
                    f"❌ Gift code `{giftcode}` already exists in the database.",
                    ephemeral=True
                )
                return
            
            # Add the code
            self.gift_operations.cursor.execute(
                "INSERT INTO gift_codes (giftcode, date, validation_status) VALUES (?, ?, ?)",
                (giftcode, datetime.now().strftime("%Y-%m-%d"), "pending")
            )
            self.gift_operations.conn.commit()
            
            embed = discord.Embed(
                title="✅ Gift Code Created",
                description=f"Gift code `{giftcode}` has been added to the database.",
                color=discord.Color.green()
            )
            
            await interaction.response.send_message(embed=embed, ephemeral=True)
            
        except Exception as e:
            self.gift_operations.logger.exception(f"Error creating gift code: {e}")
            await interaction.response.send_message(
                f"❌ Error creating gift code: {str(e)}",
                ephemeral=True
            )
