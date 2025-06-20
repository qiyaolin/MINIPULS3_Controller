import tkinter as tk
from tkinter import ttk
import ttkbootstrap as tb
from ttkbootstrap.constants import *
from ttkbootstrap.tooltip import ToolTip
from ttkbootstrap.scrolled import ScrolledFrame
from tkinter import filedialog, messagebox, font
import serial
import serial.tools.list_ports
import time
import json
import threading
import queue
import copy
import sys
import os
from datetime import timedelta
import contextlib
import csv
import math

# Check for optional libraries
try:
    from PIL import Image, ImageTk
except ImportError:
    ImageTk = None
    print("Warning: Pillow library not found. Icons will not be displayed. Please install with 'pip install Pillow'")

try:
    from matplotlib.figure import Figure
    from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
    from matplotlib.font_manager import FontProperties
except ImportError:
    FigureCanvasTkAgg = None
    print(
        "Warning: Matplotlib library not found. The plot panel will be disabled. Please install with 'pip install matplotlib'")


# --- Helper Functions & Constants ---

def resource_path(relative_path):
    """ Get absolute path to resource, works for dev and for PyInstaller """
    try:
        base_path = sys._MEIPASS
    except AttributeError:
        base_path = os.path.abspath(".")
    return os.path.join(base_path, relative_path)


CONFIG_FILE = "pump_controller_settings.json"
BACKUP_FILE = "pump_controller_backup.json"
ICON_SIZE = (20, 20)
APP_VERSION = "3.2.0"

# Constants for shear stress calculation
CHAMBER_COEFFICIENTS = {
    "µ-Slide I 0.2 Luer": 512.9,
    "µ-Slide I 0.2 Luer Glass Bottom": 330.4,
    "µ-Slide I 0.4 Luer": 131.6,
    "µ-Slide I 0.4 Luer Glass Bottom": 104.7,
    "µ-Slide I 0.6 Luer": 60.1,
    "µ-Slide I 0.6 Luer Glass Bottom": 51.5,
    "µ-Slide I 0.8 Luer": 34.7,
    "µ-Slide I 0.8 Luer Glass Bottom": 31.0,
    "µ-Slide VI 0.4": 176.1,
    "µ-Slide VI 0.5 Glass Bottom": 99.1,
    "µ-Slide VI 0.1": 10.7,
    "µ-Slide Membrane ibiPore Flow": 131.6,
    "µ-Slide I Luer 3D": 60.1,
}

DEFAULT_VISCOSITY = 0.0070
DEFAULT_TUBE_COEFFICIENT = 0.63
DEFAULT_CHAMBER = "µ-Slide VI 0.4"


# ==============================================================================
# ## Backend Controller (Unchanged) ##
# ==============================================================================
class MinipulsController:
    """ Handles all serial communication with the pump in a separate thread. """

    def __init__(self, command_queue, result_queue):
        self.command_queue, self.result_queue, self.ser, self.stop_thread = command_queue, result_queue, None, threading.Event()

    def run(self):
        while not self.stop_thread.is_set():
            try:
                command = self.command_queue.get(timeout=0.1)
                action = command.get("action")
                if action == "connect":
                    self._connect(command)
                elif action == "disconnect":
                    self._disconnect()
                elif action == "send_command":
                    self._send_command(command.get("command_str"))
                elif action == "stop_thread":
                    self._disconnect()
                    break
            except queue.Empty:
                continue

    def _connect(self, config):
        try:
            self.ser = serial.Serial(config["port"], config["baudrate"], bytesize=serial.EIGHTBITS,
                                     parity=serial.PARITY_EVEN, stopbits=serial.STOPBITS_ONE, timeout=1)
            self.ser.write(bytes([255]))
            time.sleep(0.05)
            connect_command = bytes([config["unit_id"] + 128])
            self.ser.write(connect_command)
            response = self.ser.read(1)
            if response == connect_command:
                self.result_queue.put(
                    {"status": "connected", "msg": f"Successfully connected to pump (ID: {config['unit_id']})."})
            else:
                if self.ser: self.ser.close()
                self.ser = None
                self.result_queue.put({"status": "error",
                                       "msg": f"Connection failed. Expected {connect_command.hex()} but received {response.hex()}"})
        except serial.SerialException as e:
            self.ser = None
            self.result_queue.put({"status": "error", "msg": f"Connection Error: {e}"})

    def _disconnect(self):
        if self.ser and self.ser.is_open:
            try:
                self.ser.write(bytes([255]))
                time.sleep(0.05)
                self.ser.close()
                self.result_queue.put({"status": "disconnected", "msg": "Serial port closed."})
            except Exception as e:
                self.result_queue.put({"status": "error", "msg": f"Error during disconnect: {e}"})
        self.ser = None

    def _send_command(self, command_str):
        if self.ser and self.ser.is_open:
            full_command = b'\n' + command_str.encode('ascii') + b'\r'
            self.ser.write(full_command)
            time.sleep(0.05)
        else:
            self.result_queue.put({"status": "error", "msg": "Cannot send command: Pump not connected."})


# ==============================================================================
# ## Custom Widgets ##
# ==============================================================================
class CollapsibleFrame(tb.Frame):
    """ A custom collapsible frame widget. """

    def __init__(self, parent, text="", bootstyle=DEFAULT, **kwargs):
        super().__init__(parent, **kwargs)
        self.columnconfigure(0, weight=1)
        self.bootstyle = bootstyle

        self.toggle_button = tb.Button(self, text=f"▼ {text}", bootstyle=(bootstyle, "flat"), command=self.toggle)
        self.toggle_button.grid(row=0, column=0, sticky="ew")

        self.sub_frame = tb.Frame(self)
        self.sub_frame.grid(row=1, column=0, sticky="nsew", padx=10, pady=(5, 10))
        self.is_collapsed = False

    def toggle(self):
        self.is_collapsed = not self.is_collapsed
        if self.is_collapsed:
            self.sub_frame.grid_remove()
            self.toggle_button.config(text=self.toggle_button.cget('text').replace("▼", "▶"))
        else:
            self.sub_frame.grid()
            self.toggle_button.config(text=self.toggle_button.cget('text').replace("▶", "▼"))


# ==============================================================================
# ## Dialog Windows ##
# ==============================================================================
class BaseDialog(tb.Toplevel):
    """ A base class for dialogs with common validation logic. """

    def __init__(self, parent, title=""):
        super().__init__(parent)
        self.transient(parent)
        self.title(title)
        self.result = None
        self.grab_set()
        self.geometry(f"+{parent.winfo_rootx() + 50}+{parent.winfo_rooty() + 50}")
        self.protocol("WM_DELETE_WINDOW", self.on_cancel)
        self.bind("<Escape>", self.on_cancel)

        self.vcmd_float = (self.register(self.validate_float), '%P')
        self.vcmd_int = (self.register(self.validate_int), '%P')

    def validate_float(self, val):
        if val in ["", "-"]: return True
        try:
            float(val)
            return True
        except ValueError:
            return False

    def validate_int(self, val):
        if val in ["", "-"]: return True
        try:
            int(val)
            return True
        except ValueError:
            return False

    def on_cancel(self, event=None):
        self.result = None
        self.destroy()


class AddPhaseDialog(BaseDialog):
    """ Dialog to add or edit a sequence phase. """

    def __init__(self, parent, initial_data=None):
        super().__init__(parent, title="Add/Edit Phase")
        self.initial_data = initial_data or {}

        body = tb.Frame(self, padding=15)
        body.pack(fill=BOTH, expand=True)
        body.columnconfigure(1, weight=1)

        self.controls = {}
        widgets_map = {
            "Direction:": ("direction", tb.Combobox(body, values=["Forward", "Backward"], state="readonly")),
            "Speed Mode:": ("speed_mode", tb.Combobox(body, values=["Fixed", "Ramp"], state="readonly")),
            "Target RPM:": ("target_rpm", tb.Entry(body, validate="key", validatecommand=self.vcmd_float)),
            "Duration:": ("duration", tb.Entry(body, validate="key", validatecommand=self.vcmd_float)),
            "Unit:": ("unit", tb.Combobox(body, values=["s", "min", "hr"], state="readonly")),
        }

        for i, (text, (key, widget)) in enumerate(widgets_map.items()):
            tb.Label(body, text=text).grid(row=i, column=0, sticky='w', pady=5)
            widget.grid(row=i, column=1, sticky='ew', pady=5, padx=5)
            self.controls[key] = widget

        self.update_interval_label = tb.Label(body, text="Update Interval (s):")
        self.controls['update_interval'] = tb.Entry(body, validate="key", validatecommand=self.vcmd_float)

        self.controls['speed_mode'].bind("<<ComboboxSelected>>", self._toggle_interval_entry)

        self._setup_tooltips()
        self._populate_initial_data()
        self._toggle_interval_entry()

        btn_frame = tb.Frame(self)
        btn_frame.pack(fill='x', padx=15, pady=(5, 15))
        btn_frame.columnconfigure((0, 1), weight=1)
        tb.Button(btn_frame, text="OK", command=self.on_ok, bootstyle="success").grid(row=0, column=0, sticky=EW,
                                                                                      padx=(0, 5))
        tb.Button(btn_frame, text="Cancel", command=self.on_cancel, bootstyle="secondary").grid(row=0, column=1,
                                                                                                sticky=EW, padx=(5, 0))
        self.wait_window(self)

    def _setup_tooltips(self):
        ToolTip(self.controls['direction'], text="Pump rotation direction.", bootstyle="info")
        ToolTip(self.controls['speed_mode'], text="Fixed: Constant speed.\nRamp: Linearly change to target speed.",
                bootstyle="info")
        ToolTip(self.controls['target_rpm'], text="Target Revolutions Per Minute (0-48).", bootstyle="info")
        ToolTip(self.controls['duration'], text="How long this phase will last.", bootstyle="info")
        ToolTip(self.controls['update_interval'], text="For Ramp mode. How often to send a new speed command.",
                bootstyle="info")

    def _populate_initial_data(self):
        self.controls['direction'].set(self.initial_data.get("direction", "Forward"))
        self.controls['speed_mode'].set(self.initial_data.get("mode", "Fixed"))
        self.controls['target_rpm'].insert(0, str(self.initial_data.get("rpm", 10.0)))
        self.controls['duration'].insert(0, str(self.initial_data.get("duration", 60)))
        self.controls['unit'].set(self.initial_data.get("unit", "s"))
        self.controls['update_interval'].insert(0, str(self.initial_data.get("update_interval", 1.0)))

    def _toggle_interval_entry(self, event=None):
        if self.controls['speed_mode'].get() == "Ramp":
            self.update_interval_label.grid(row=5, column=0, sticky='w', pady=5)
            self.controls['update_interval'].grid(row=5, column=1, sticky='ew', pady=5, padx=5)
        else:
            self.update_interval_label.grid_remove()
            self.controls['update_interval'].grid_remove()

    def on_ok(self):
        try:
            rpm = float(self.controls['target_rpm'].get())
            duration = float(self.controls['duration'].get())
            if not (0 <= rpm <= 48 and duration >= 0): raise ValueError("Invalid RPM or duration value")

            self.result = {
                "type": "Phase",
                "direction": self.controls['direction'].get(),
                "mode": self.controls['speed_mode'].get(),
                "rpm": rpm,
                "duration": duration,
                "unit": self.controls['unit'].get(),
                "enabled": self.initial_data.get("enabled", True)
            }
            if self.result["mode"] == "Ramp":
                update_interval = float(self.controls['update_interval'].get())
                if update_interval <= 0: raise ValueError("Update interval must be positive")
                self.result["update_interval"] = update_interval
            self.destroy()
        except Exception as e:
            messagebox.showerror("Input Error", f"Please check all input values.\n\nDetails: {e}", parent=self)


class AddCycleDialog(BaseDialog):
    """ Dialog to add or edit a sequence cycle. """

    def __init__(self, parent, phase_indices, initial_data=None):
        super().__init__(parent, title="Add/Edit Cycle")
        self.initial_data, self.phase_indices = initial_data or {}, phase_indices

        body = tb.Frame(self, padding=15)
        body.pack(fill=BOTH, expand=True)

        if not self.phase_indices:
            tb.Label(body, text="No phases available to create a cycle.", bootstyle="danger").pack(pady=10)
            tb.Button(body, text="OK", command=self.on_cancel, bootstyle="secondary").pack(pady=10)
            self.wait_window(self)
            return

        body.columnconfigure(1, weight=1)
        self.start_cb = tb.Combobox(body, values=self.phase_indices, state="readonly")
        self.end_cb = tb.Combobox(body, values=self.phase_indices, state="readonly")
        self.repeats_entry = tb.Entry(body, validate="key", validatecommand=self.vcmd_int)

        tb.Label(body, text="Start Phase #:").grid(row=0, column=0, sticky='w', pady=5)
        self.start_cb.grid(row=0, column=1, sticky='ew', pady=5, padx=5)
        tb.Label(body, text="End Phase #:").grid(row=1, column=0, sticky='w', pady=5)
        self.end_cb.grid(row=1, column=1, sticky='ew', pady=5, padx=5)
        tb.Label(body, text="Number of Repeats:").grid(row=2, column=0, sticky='w', pady=5)
        self.repeats_entry.grid(row=2, column=1, sticky='ew', pady=5, padx=5)

        ToolTip(self.start_cb, "The first phase number in the loop.", bootstyle="info")
        ToolTip(self.end_cb, "The last phase number in the loop.", bootstyle="info")
        ToolTip(self.repeats_entry, "How many times to repeat this cycle.", bootstyle="info")

        self._populate_initial_data()

        btn_frame = tb.Frame(self)
        btn_frame.pack(fill='x', padx=15, pady=(5, 15))
        btn_frame.columnconfigure((0, 1), weight=1)
        tb.Button(btn_frame, text="OK", command=self.on_ok, bootstyle="success").grid(row=0, column=0, sticky=EW,
                                                                                      padx=(0, 5))
        tb.Button(btn_frame, text="Cancel", command=self.on_cancel, bootstyle="secondary").grid(row=0, column=1,
                                                                                                sticky=EW, padx=(5, 0))
        self.wait_window(self)

    def _populate_initial_data(self):
        start = self.initial_data.get("start_phase", self.phase_indices[0] if self.phase_indices else 1)
        self.start_cb.set(
            start if start in self.phase_indices else (self.phase_indices[0] if self.phase_indices else 1))
        end = self.initial_data.get("end_phase", self.phase_indices[-1] if self.phase_indices else 1)
        self.end_cb.set(end if end in self.phase_indices else (self.phase_indices[-1] if self.phase_indices else 1))
        self.repeats_entry.insert(0, str(self.initial_data.get("repeats", 3)))

    def on_ok(self):
        try:
            start, end, repeats = int(self.start_cb.get()), int(self.end_cb.get()), int(self.repeats_entry.get())
            if not (start > 0 and end >= start and repeats > 0): raise ValueError("End Phase must be >= Start Phase.")
            self.result = {"type": "Cycle", "start_phase": start, "end_phase": end, "repeats": repeats,
                           "enabled": self.initial_data.get("enabled", True)}
            self.destroy()
        except Exception as e:
            messagebox.showerror("Input Error",
                                 f"Please check all values.\nEnd Phase must be >= Start Phase.\n\nDetails: {e}",
                                 parent=self)


class BatchEditDialog(BaseDialog):
    """ Dialog to batch edit multiple Phase steps. """

    def __init__(self, parent, num_items):
        super().__init__(parent, title=f"Batch Edit {num_items} Phases")
        self.num_items = num_items

        body = tb.Frame(self, padding=15)
        body.pack(fill=BOTH, expand=True)
        body.columnconfigure(1, weight=1)

        self.vars = {key: tk.BooleanVar() for key in ['direction', 'speed_mode', 'rpm', 'duration', 'unit']}
        self.controls = {}

        # Direction
        f1 = tb.Frame(body)
        f1.grid(row=0, column=1, sticky='ew', pady=5, padx=5)
        self.controls['direction'] = tb.Combobox(f1, values=["Forward", "Backward"], state="readonly", width=10)
        self.controls['direction'].pack(side=LEFT, fill=X, expand=True)

        # Speed Mode
        f2 = tb.Frame(body)
        f2.grid(row=1, column=1, sticky='ew', pady=5, padx=5)
        self.controls['speed_mode'] = tb.Combobox(f2, values=["Fixed", "Ramp"], state="readonly", width=10)
        self.controls['speed_mode'].pack(side=LEFT, fill=X, expand=True)

        # RPM
        f3 = tb.Frame(body)
        f3.grid(row=2, column=1, sticky='ew', pady=5, padx=5)
        self.controls['rpm'] = tb.Entry(f3, validate="key", validatecommand=self.vcmd_float)
        self.controls['rpm'].pack(side=LEFT, fill=X, expand=True)

        # Duration
        f4 = tb.Frame(body)
        f4.grid(row=3, column=1, sticky='ew', pady=5, padx=5)
        self.controls['duration'] = tb.Entry(f4, validate="key", validatecommand=self.vcmd_float)
        self.controls['duration'].pack(side=LEFT, fill=X, expand=True)

        # Unit
        f5 = tb.Frame(body)
        f5.grid(row=4, column=1, sticky='ew', pady=5, padx=5)
        self.controls['unit'] = tb.Combobox(f5, values=["s", "min", "hr"], state="readonly", width=10)
        self.controls['unit'].pack(side=LEFT, fill=X, expand=True)

        # Checkbuttons and Labels
        for i, (key, text) in enumerate(
                [('direction', 'Direction:'), ('speed_mode', 'Speed Mode:'), ('rpm', 'Target RPM:'),
                 ('duration', 'Duration:'), ('unit', 'Unit:')]):
            cb = tb.Checkbutton(body, variable=self.vars[key], command=lambda k=key: self.toggle_control(k))
            cb.grid(row=i, column=0, sticky='w', pady=5)
            tb.Label(body, text=text).grid(row=i, column=0, sticky='w', pady=5, padx=(25, 0))
            self.toggle_control(key)  # Initial state

        btn_frame = tb.Frame(self)
        btn_frame.pack(fill='x', padx=15, pady=(15, 15))
        btn_frame.columnconfigure((0, 1), weight=1)
        tb.Button(btn_frame, text="Apply Changes", command=self.on_ok, bootstyle="success").grid(row=0, column=0,
                                                                                                 sticky=EW,
                                                                                                 padx=(0, 5))
        tb.Button(btn_frame, text="Cancel", command=self.on_cancel, bootstyle="secondary").grid(row=0, column=1,
                                                                                                sticky=EW, padx=(5, 0))
        self.wait_window(self)

    def toggle_control(self, key):
        """Enable/disable control based on checkbox."""
        state = NORMAL if self.vars[key].get() else DISABLED
        self.controls[key].config(state=state)

    def on_ok(self):
        self.result = {}
        try:
            for key, var in self.vars.items():
                if var.get():
                    widget = self.controls[key]
                    value = widget.get()
                    if not value:
                        raise ValueError(f"'{key}' cannot be empty.")
                    if key in ['rpm', 'duration']:
                        value = float(value)
                        if key == 'rpm' and not (0 <= value <= 48): raise ValueError("RPM must be between 0-48.")
                        if key == 'duration' and value < 0: raise ValueError("Duration must be non-negative.")
                    self.result[key] = value
            self.destroy()
        except Exception as e:
            messagebox.showerror("Input Error", f"Please check your input.\n\nDetails: {e}", parent=self)


class SettingsDialog(BaseDialog):
    """ Dialog for application settings. """

    def __init__(self, parent, settings):
        super().__init__(parent, "Settings")
        self.settings = copy.deepcopy(settings)  # Work on a copy

        notebook = tb.Notebook(self, padding=10)
        notebook.pack(fill=BOTH, expand=True)

        # Create tabs
        f_general = tb.Frame(notebook, padding=10)
        f_pump = tb.Frame(notebook, padding=10)
        f_fonts = tb.Frame(notebook, padding=10)
        f_templates = tb.Frame(notebook, padding=10)

        notebook.add(f_general, text="General")
        notebook.add(f_pump, text="Pump Parameters")
        notebook.add(f_fonts, text="Fonts")
        notebook.add(f_templates, text="Templates")

        # --- General Tab ---
        f_general.columnconfigure(1, weight=1)
        tb.Label(f_general, text="Theme:").grid(row=0, column=0, sticky='w', pady=5)
        self.theme_cb = tb.Combobox(f_general, state="readonly", values=parent.style.theme_names())
        self.theme_cb.set(self.settings.get("theme", "litera"))
        self.theme_cb.grid(row=0, column=1, sticky='ew', pady=5, padx=5)

        tb.Label(f_general, text="Auto-save Interval (min):").grid(row=1, column=0, sticky='w', pady=5)
        self.autosave_entry = tb.Entry(f_general, validate="key", validatecommand=self.vcmd_int)
        self.autosave_entry.insert(0, str(self.settings.get("autosave_interval_min", 5)))
        self.autosave_entry.grid(row=1, column=1, sticky='ew', pady=5, padx=5)

        # --- Pump Tab ---
        f_pump.columnconfigure(1, weight=1)
        tb.Label(f_pump, text="Tube Inner Diameter (mm):").grid(row=0, column=0, sticky='w', pady=5)
        self.tube_id_entry = tb.Entry(f_pump, validate="key", validatecommand=self.vcmd_float)
        self.tube_id_entry.insert(0, str(self.settings.get("tube_inner_diameter_mm", 1.0)))
        self.tube_id_entry.grid(row=0, column=1, sticky='ew', pady=5, padx=5)

        tb.Label(f_pump, text="Volume per Revolution (µL):").grid(row=1, column=0, sticky='w', pady=5)
        self.vol_per_rev_entry = tb.Entry(f_pump, validate="key", validatecommand=self.vcmd_float)
        self.vol_per_rev_entry.insert(0, str(self.settings.get("vol_per_rev_ul", 25.0)))
        self.vol_per_rev_entry.grid(row=1, column=1, sticky='ew', pady=5, padx=5)

        tb.Label(f_pump, text="Dynamic Viscosity η:").grid(row=2, column=0, sticky='w', pady=5)
        self.viscosity_entry = tb.Entry(f_pump, validate="key", validatecommand=self.vcmd_float)
        self.viscosity_entry.insert(0, str(self.settings.get("dynamic_viscosity", DEFAULT_VISCOSITY)))
        self.viscosity_entry.grid(row=2, column=1, sticky='ew', pady=5, padx=5)

        tb.Label(f_pump, text="Tube Coefficient k:").grid(row=3, column=0, sticky='w', pady=5)
        self.tube_coeff_entry = tb.Entry(f_pump, validate="key", validatecommand=self.vcmd_float)
        self.tube_coeff_entry.insert(0, str(self.settings.get("tube_coefficient", DEFAULT_TUBE_COEFFICIENT)))
        self.tube_coeff_entry.grid(row=3, column=1, sticky='ew', pady=5, padx=5)

        tb.Label(f_pump, text="Chamber Type:").grid(row=4, column=0, sticky='w', pady=5)
        chamber_names = list(CHAMBER_COEFFICIENTS.keys())
        self.chamber_cb = tb.Combobox(f_pump, values=chamber_names, state="readonly")
        self.chamber_cb.set(self.settings.get("chamber_type", DEFAULT_CHAMBER))
        self.chamber_cb.grid(row=4, column=1, sticky='ew', pady=5, padx=5)
        self.chamber_cb.bind("<<ComboboxSelected>>", self._update_chamber_p)

        tb.Label(f_pump, text="Chamber p:").grid(row=5, column=0, sticky='w', pady=5)
        self.chamber_p_entry = tb.Entry(f_pump, validate="key", validatecommand=self.vcmd_float)
        self.chamber_p_entry.insert(0, str(self.settings.get("chamber_p_value", CHAMBER_COEFFICIENTS.get(self.chamber_cb.get(), 176.1))))
        self.chamber_p_entry.grid(row=5, column=1, sticky='ew', pady=5, padx=5)

        # --- Fonts Tab ---
        f_fonts.columnconfigure(1, weight=1)
        self.font_vars = {}
        font_settings = self.settings.get("fonts", {"default": 10, "title": 12, "plot_title": 12, "editor": 11})

        for i, (key, text) in enumerate(
                [
                    ('default', 'Default Size:'),
                    ('title', 'Panel Title Size:'),
                    ('plot_title', 'Plot Title Size:'),
                    ('editor', 'Editor Size:')
                ]):
            tb.Label(f_fonts, text=text).grid(row=i, column=0, sticky='w', pady=5)
            self.font_vars[key] = tk.IntVar(value=font_settings.get(key, 10))
            spinbox = tb.Spinbox(f_fonts, from_=8, to=24, textvariable=self.font_vars[key], width=5)
            spinbox.grid(row=i, column=1, sticky='w', pady=5, padx=5)

        # --- Templates Tab ---
        f_templates.columnconfigure(0, weight=1)
        tb.Label(f_templates, text="Manage saved sequence templates:").pack(anchor='w')
        list_frame = tb.Frame(f_templates)
        list_frame.pack(fill=BOTH, expand=True, pady=5)
        self.template_list = tk.Listbox(list_frame)
        self.template_list.pack(side=LEFT, fill=BOTH, expand=True)
        vsb = tb.Scrollbar(list_frame, orient=VERTICAL, command=self.template_list.yview)
        vsb.pack(side=RIGHT, fill=Y)
        self.template_list.config(yscrollcommand=vsb.set)

        self.templates = self.settings.get("templates", {})
        for name in sorted(self.templates.keys()):
            self.template_list.insert(END, name)

        btn_frame_templates = tb.Frame(f_templates)
        btn_frame_templates.pack(fill=X, pady=5)
        tb.Button(btn_frame_templates, text="Delete Selected", command=self.delete_template, bootstyle="danger").pack(
            side=LEFT)

        # --- Bottom Buttons ---
        btn_frame = tb.Frame(self)
        btn_frame.pack(fill='x', padx=15, pady=(5, 15))
        btn_frame.columnconfigure((0, 1), weight=1)
        tb.Button(btn_frame, text="Save Settings", command=self.on_ok, bootstyle="success").grid(row=0, column=0,
                                                                                                 sticky=EW,
                                                                                                 padx=(0, 5))
        tb.Button(btn_frame, text="Cancel", command=self.on_cancel, bootstyle="secondary").grid(row=0, column=1,
                                                                                                sticky=EW, padx=(5, 0))
        self.wait_window(self)

    def delete_template(self):
        selected_indices = self.template_list.curselection()
        if not selected_indices:
            messagebox.showwarning("No Selection", "Please select a template to delete.", parent=self)
            return

        selected_template = self.template_list.get(selected_indices[0])
        if messagebox.askyesno("Confirm Deletion",
                               f"Are you sure you want to delete the template '{selected_template}'?", parent=self):
            del self.templates[selected_template]
            self.template_list.delete(selected_indices[0])

    def _update_chamber_p(self, event=None):
        sel = self.chamber_cb.get()
        if sel in CHAMBER_COEFFICIENTS:
            self.chamber_p_entry.delete(0, END)
            self.chamber_p_entry.insert(0, str(CHAMBER_COEFFICIENTS[sel]))

    def on_ok(self):
        try:
            self.settings["theme"] = self.theme_cb.get()
            self.settings["autosave_interval_min"] = int(self.autosave_entry.get())
            self.settings["tube_inner_diameter_mm"] = float(self.tube_id_entry.get())
            self.settings["vol_per_rev_ul"] = float(self.vol_per_rev_entry.get())
            self.settings["dynamic_viscosity"] = float(self.viscosity_entry.get())
            self.settings["tube_coefficient"] = float(self.tube_coeff_entry.get())
            self.settings["chamber_type"] = self.chamber_cb.get()
            self.settings["chamber_p_value"] = float(self.chamber_p_entry.get())
            self.settings["fonts"] = {key: var.get() for key, var in self.font_vars.items()}
            self.settings["templates"] = self.templates  # Save updated templates
            self.result = self.settings
            self.destroy()
        except Exception as e:
            messagebox.showerror("Input Error", f"Please check all settings values.\n\nDetails: {e}", parent=self)


class AboutDialog(BaseDialog):
    """ 'About' dialog window. """

    def __init__(self, parent):
        super().__init__(parent, title="About Minipuls 3 Controller")

        body = tb.Frame(self, padding=20)
        body.pack(fill=BOTH, expand=True)

        try:
            icon_path = resource_path(os.path.join('icons', 'minipuls3_icon.ico'))
            img = Image.open(icon_path)
            img = img.resize((64, 64), Image.LANCZOS)
            self.app_icon = ImageTk.PhotoImage(img)
            tb.Label(body, image=self.app_icon).pack(pady=(0, 10))
        except Exception as e:
            print(f"Warning: Could not load icon for About dialog: {e}")

        tb.Label(body, text=f"Minipuls 3 Advanced Controller", font=parent.title_font).pack()
        tb.Label(body, text=f"Version {APP_VERSION}", font=parent.small_font).pack()
        tb.Separator(body, orient=HORIZONTAL).pack(fill=X, pady=15)
        tb.Label(body, text="A modern GUI for controlling Gilson Minipuls 3 pumps.", justify=CENTER).pack(pady=5)
        tb.Label(body, text="Created by: Qiyao Lin", font=parent.small_font).pack()
        tb.Label(body, text="Contact: qiyaolin3776@gmail.com", font=parent.small_font).pack(pady=(0, 15))

        ok_button = tb.Button(body, text="OK", command=self.on_cancel, bootstyle="primary")
        ok_button.pack()
        ok_button.focus_set()
        self.wait_window(self)


# ==============================================================================
# ## Main GUI Application ##
# ==============================================================================
class PumpControlUI(tb.Window):
    def __init__(self, themename='litera'):
        # --- Load Settings First ---
        self.settings = self._load_settings()

        super().__init__(themename=self.settings.get("theme", themename))
        self.withdraw()

        # --- Initialize Backend ---
        self.command_queue, self.result_queue = queue.Queue(), queue.Queue()
        self.pump_controller = MinipulsController(self.command_queue, self.result_queue)
        self.controller_thread = threading.Thread(target=self.pump_controller.run, daemon=True)
        self.controller_thread.start()

        # --- State Variables ---
        self.sequence_thread, self.stop_event, self.pause_event = None, threading.Event(), threading.Event()
        self.sequence_data, self.current_filepath, self.is_dirty, self.clipboard = [], None, False, None
        self.is_connected, self.is_running_sequence, self.is_paused = False, False, False
        self.plot_is_live, self.actual_plot_line, self.actual_plot_data, self.last_plot_direction = False, None, {
            "time": [], "rpm": []}, None
        self.autosave_timer_id = None

        self.geometry(self.settings.get("geometry", "1350x850"))

        # --- UI Setup ---
        self.title("Minipuls 3 Advanced Controller")
        try:
            icon_path = resource_path(os.path.join('icons', 'minipuls3_icon.ico'))
            self.iconbitmap(icon_path)
        except Exception as e:
            print(f"Warning: Could not set icon: {e}")

        # MODIFIED: Correct initialization order
        self._initialize_fonts()
        self._load_icons()
        self._create_widgets()
        self._configure_styles()
        self._create_menu()
        self._setup_shortcuts()

        self.protocol("WM_DELETE_WINDOW", self._on_closing)
        self._update_com_ports(auto_select=True)
        self._process_results()

        self.after(100, self._update_ui_states)  # Final UI state update
        self._check_for_recovery()
        self._schedule_autosave()
        self.deiconify()

    # --- Settings, Resources & Recovery ---
    def _load_settings(self):
        try:
            with open(CONFIG_FILE, 'r') as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            return {
                "recent_files": [],
                "templates": {},
                "fonts": {"default": 10, "title": 12, "plot_title": 12, "editor": 11},
                "dynamic_viscosity": DEFAULT_VISCOSITY,
                "tube_coefficient": DEFAULT_TUBE_COEFFICIENT,
                "chamber_type": DEFAULT_CHAMBER,
                "chamber_p_value": CHAMBER_COEFFICIENTS.get(DEFAULT_CHAMBER, 176.1),
            }

    def _save_settings(self):
        self.settings['theme'] = self.style.theme.name
        self.settings['geometry'] = self.geometry()
        try:
            self.settings['main_pane'] = self.main_pane.sashpos(0)
            self.settings['right_pane'] = self.right_pane.sashpos(0)
        except (tk.TclError, AttributeError):
            pass
        with open(CONFIG_FILE, 'w') as f:
            json.dump(self.settings, f, indent=4)

    def _add_to_recent_files(self, filepath):
        if 'recent_files' not in self.settings:
            self.settings['recent_files'] = []
        if filepath in self.settings['recent_files']:
            self.settings['recent_files'].remove(filepath)
        self.settings['recent_files'].insert(0, filepath)
        self.settings['recent_files'] = self.settings['recent_files'][:10]  # Keep last 10
        self._update_recent_files_menu()

    def _check_for_recovery(self):
        if os.path.exists(BACKUP_FILE):
            if messagebox.askyesno("Recovery", "An unsaved session was found. Do you want to recover it?"):
                try:
                    with open(BACKUP_FILE, 'r') as f:
                        recovered_data = json.load(f)
                        self.sequence_data = recovered_data
                        self._mark_dirty(True)
                        self._update_treeview()
                        self._log("Session recovered from backup.", "INFO")
                except Exception as e:
                    messagebox.showerror("Recovery Failed", f"Could not load backup file: {e}")
            # Clean up backup file regardless of choice
            with contextlib.suppress(OSError):
                os.remove(BACKUP_FILE)

    def _schedule_autosave(self):
        if self.autosave_timer_id:
            self.after_cancel(self.autosave_timer_id)

        interval_min = self.settings.get("autosave_interval_min", 5)
        if interval_min > 0:
            interval_ms = interval_min * 60 * 1000
            self.autosave_timer_id = self.after(interval_ms, self._perform_autosave)

    def _perform_autosave(self):
        if self.is_dirty:
            try:
                with open(BACKUP_FILE, 'w') as f:
                    json.dump(self.sequence_data, f)
                self._log(f"Work auto-saved to backup file.", "INFO")
            except Exception as e:
                self._log(f"Auto-save failed: {e}", "ERROR")
        self._schedule_autosave()  # Schedule next one

    def _load_icons(self):
        self.icons = {k: None for k in
                      ['add', 'cycle', 'remove', 'clear', 'up', 'down', 'save', 'load', 'exit', 'light', 'dark',
                       'refresh', 'edit', 'copy', 'paste', 'duplicate', 'play', 'stop', 'new_file', 'help', 'about',
                       'pause', 'settings', 'template_add', 'template_load', 'export_img', 'export_csv', 'batch_edit',
                       'toggle_on', 'toggle_off']}
        if not ImageTk: return

        icon_map = {
            'new_file': 'file-earmark-plus.png', 'load': 'folder2-open.png', 'save': 'save.png',
            'exit': 'box-arrow-right.png',
            'light': 'sun.png', 'dark': 'moon-stars.png', 'help': 'question-circle.png', 'about': 'info-circle.png',
            'add': 'plus-circle-dotted.png', 'cycle': 'arrow-repeat.png', 'edit': 'pencil-square.png',
            'remove': 'trash.png', 'clear': 'x-octagon.png', 'up': 'arrow-up-circle.png',
            'down': 'arrow-down-circle.png', 'copy': 'clipboard.png', 'paste': 'clipboard-plus.png',
            'duplicate': 'files.png', 'play': 'play-circle.png', 'stop': 'stop-circle.png',
            'refresh': 'arrow-clockwise.png', 'pause': 'pause-circle.png', 'settings': 'gear.png',
            'template_add': 'bookmark-plus.png', 'template_load': 'bookmark-check.png',
            'export_img': 'image.png', 'export_csv': 'file-earmark-ruled.png',
            'batch_edit': 'pencil-fill.png', 'toggle_on': 'toggle-on.png', 'toggle_off': 'toggle-off.png'
        }

        for name, filename in icon_map.items():
            try:
                path = resource_path(os.path.join('icons', filename))
                img = Image.open(path).convert("RGBA").resize(ICON_SIZE, Image.LANCZOS)
                self.icons[name] = ImageTk.PhotoImage(img)
            except Exception as e:
                print(f"Warning: Could not load icon '{filename}': {e}")

    def _initialize_fonts(self):
        """Initializes font objects based on settings."""
        fonts = self.settings.get("fonts", {"default": 10, "title": 12, "plot_title": 12, "editor": 11})
        base_size = fonts.get("default", 10)
        title_size = fonts.get("title", 12)
        plot_title_size = fonts.get("plot_title", title_size)
        editor_size = fonts.get("editor", 11)

        self.default_font = font.nametofont("TkDefaultFont")
        self.default_font.configure(size=base_size)
        self.title_font = font.Font(family="Segoe UI", size=title_size, weight="bold")
        self.status_font = font.Font(family="Segoe UI", size=base_size, weight="bold")
        self.small_font = font.Font(family="Segoe UI", size=base_size - 1)
        self.mono_font = font.Font(family="Consolas", size=base_size)
        self.editor_font = font.Font(family="Segoe UI", size=editor_size)
        self.plot_title_font = font.Font(family="Segoe UI", size=plot_title_size, weight="bold")

    def _configure_styles(self):
        """Configures the ttkbootstrap style system with custom fonts."""
        fonts = self.settings.get("fonts", {"default": 10, "title": 12, "plot_title": 12, "editor": 11})
        editor_size = fonts.get("editor", 11)

        # Ensure label frame titles honor the Panel Title Size setting
        self.style.configure('TLabelframe.Label', font=self.title_font)
        self.style.configure('Treeview', rowheight=int(editor_size * 2.5), font=self.editor_font,
                             bordercolor=self.style.colors.light, borderwidth=1, relief='solid')
        # Style with light grid lines for the sequence editor
        self.style.configure('Table.Treeview', rowheight=int(editor_size * 2.5), font=self.editor_font,
                             bordercolor=self.style.colors.light, borderwidth=1, relief='solid',
                             rowbordercolor=self.style.colors.light, rowborderwidth=1)
        self.style.configure('Treeview.Heading', font=self.title_font)
        self.style.map('Treeview', background=[('selected', self.style.colors.primary)])
        self.style.configure("Disabled.Treeview", foreground='gray')

    def _update_styles_and_widgets(self):
        """Re-initializes fonts, re-configures styles, and updates widgets."""
        self._initialize_fonts()
        self._configure_styles()
        # Update widgets that depend on these styles
        if hasattr(self, 'sequence_tree'):
            self._update_treeview()
        if hasattr(self, 'ax'):
            self._update_plot_style()
        # You may need to update other specific widgets here if they use fonts directly
        self.step_time_label.config(font=self.small_font)
        self.total_duration_label.config(font=self.small_font)
        self.dyn_label.config(font=self.small_font)
        self.status_info.config(font=self.small_font)
        # Update log text font if necessary
        self.log_text.config(font=self.small_font)

    def _update_plot_style(self):
        if not hasattr(self, 'ax'): return
        try:
            colors = self.style.colors
            bg, fg, grid_c = colors.get('bg'), colors.get('fg'), colors.get('light')

            default_fp = FontProperties(family=self.default_font.cget("family"), size=self.default_font.cget("size"))
            title_fp = FontProperties(family=self.plot_title_font.cget("family"), size=self.plot_title_font.cget("size"),
                                      weight=self.plot_title_font.cget("weight"))

            self.fig.set_facecolor(bg)
            self.ax.set_facecolor(bg)
            for spine in self.ax.spines.values(): spine.set_color(fg)
            self.ax.tick_params(axis='x', colors=fg, labelsize=self.small_font.cget("size"))
            self.ax.tick_params(axis='y', colors=fg, labelsize=self.small_font.cget("size"))
            self.ax.yaxis.label.set_color(fg);
            self.ax.yaxis.label.set_fontproperties(default_fp)
            self.ax.xaxis.label.set_color(fg);
            self.ax.xaxis.label.set_fontproperties(default_fp)
            self.ax.title.set_color(fg);
            self.ax.title.set_fontproperties(title_fp)
            self.ax.grid(True, linestyle='--', color=grid_c, alpha=0.6)
            self.canvas.draw()
        except Exception as e:
            print(f"Warning: Failed to update plot style: {e}")

    # --- UI Creation ---
    def _create_widgets(self):
        self.main_pane = tb.PanedWindow(self, orient=HORIZONTAL)
        self.main_pane.pack(fill=BOTH, expand=True, padx=10, pady=(5, 0))

        left_wrapper = tb.Frame(self.main_pane)
        self.main_pane.add(left_wrapper, weight=1)

        left_panel = ScrolledFrame(left_wrapper, autohide=True)
        left_panel.pack(fill=BOTH, expand=TRUE)

        self._create_connection_panel(left_panel.container)
        self._create_execution_panel(left_panel.container)
        self._create_manual_control_panel(left_panel.container)

        # Log panel should remain visible at the bottom of the left side
        self._create_log_panel(left_wrapper)

        self.main_pane.after(100, lambda: self.main_pane.sashpos(0, self.settings.get("main_pane", 400)))

        self.right_pane = tb.PanedWindow(self.main_pane, orient=VERTICAL)
        self.main_pane.add(self.right_pane, weight=4)

        top_right_frame = tb.Frame(self.right_pane)
        self.right_pane.add(top_right_frame, weight=3)

        bottom_right_frame = tb.Frame(self.right_pane)
        self.right_pane.add(bottom_right_frame, weight=2)

        self._create_sequence_editor(top_right_frame)
        if FigureCanvasTkAgg:
            self._create_plot_panel(bottom_right_frame)
        else:
            tb.Label(bottom_right_frame, text="Matplotlib not available", padding=10).pack(fill=BOTH, expand=True)

        self.right_pane.after(100, lambda: self.right_pane.sashpos(0, self.settings.get("right_pane", 450)))
        self._create_status_bar()

    def _create_menu(self):
        menu_bar = tb.Menu(self)
        menu_bar.configure(font=self.title_font)
        self.config(menu=menu_bar)

        # File Menu
        file_menu = tb.Menu(menu_bar, tearoff=0)
        file_menu.add_command(label=" New Sequence", image=self.icons['new_file'], compound=LEFT,
                              command=self._clear_sequence, accelerator="Ctrl+N")
        file_menu.add_command(label=" Load Sequence", image=self.icons['load'], compound=LEFT,
                              command=self._load_sequence_dialog, accelerator="Ctrl+O")
        self.recent_files_menu = tb.Menu(file_menu, tearoff=0)
        file_menu.add_cascade(label=" Open Recent", menu=self.recent_files_menu)
        self._update_recent_files_menu()

        file_menu.add_command(label=" Save Sequence", image=self.icons['save'], compound=LEFT,
                              command=self._save_sequence, accelerator="Ctrl+S")
        file_menu.add_command(label=" Save Sequence As...", command=self._save_sequence_as, accelerator="Ctrl+Shift+S")
        file_menu.add_separator()
        file_menu.add_command(label=" Settings...", image=self.icons['settings'], compound=LEFT,
                              command=self._open_settings)
        file_menu.add_separator()
        file_menu.add_command(label=" Exit", image=self.icons['exit'], compound=LEFT, command=self._on_closing)
        menu_bar.add_cascade(label="File", menu=file_menu)

        # Edit Menu
        edit_menu = tb.Menu(menu_bar, tearoff=0)
        edit_menu.add_command(label=" Copy", image=self.icons['copy'], compound=LEFT, command=self._copy_item,
                              accelerator="Ctrl+C")
        edit_menu.add_command(label=" Paste", image=self.icons['paste'], compound=LEFT, command=self._paste_item,
                              accelerator="Ctrl+V")
        edit_menu.add_command(label=" Duplicate", image=self.icons['duplicate'], compound=LEFT,
                              command=self._duplicate_item, accelerator="Ctrl+D")
        edit_menu.add_separator()
        edit_menu.add_command(label=" Batch Edit Phases...", image=self.icons['batch_edit'], compound=LEFT,
                              command=self._batch_edit_items)
        menu_bar.add_cascade(label="Edit", menu=edit_menu)

        # Templates Menu
        self.templates_menu = tb.Menu(menu_bar, tearoff=0)
        self.templates_menu.add_command(label=" Save Selection as Template...", image=self.icons['template_add'],
                                        compound=LEFT, command=self._save_selection_as_template)
        self.templates_menu.add_separator()
        self.insert_template_menu = tb.Menu(self.templates_menu, tearoff=0)
        self.templates_menu.add_cascade(label=" Insert Template", menu=self.insert_template_menu,
                                        image=self.icons['template_load'], compound=LEFT)
        self._update_templates_menu()
        menu_bar.add_cascade(label="Templates", menu=self.templates_menu)

        # View Menu
        view_menu = tb.Menu(menu_bar, tearoff=0)
        theme_submenu = tb.Menu(view_menu, tearoff=0)
        for theme in self.style.theme_names():
            theme_submenu.add_command(label=theme.capitalize(), command=lambda t=theme: self._set_theme(t))
        view_menu.add_cascade(label="Theme", menu=theme_submenu)
        menu_bar.add_cascade(label="View", menu=view_menu)

        # Help Menu
        help_menu = tb.Menu(menu_bar, tearoff=0)
        help_menu.add_command(label=" About", image=self.icons['about'], compound=LEFT,
                              command=lambda: AboutDialog(self))
        menu_bar.add_cascade(label="Help", menu=help_menu)

    def _update_recent_files_menu(self):
        self.recent_files_menu.delete(0, END)
        recent_files = self.settings.get("recent_files", [])
        for path in recent_files:
            filename = os.path.basename(path)
            self.recent_files_menu.add_command(label=filename, command=lambda p=path: self._load_sequence(p))
        if not recent_files:
            self.recent_files_menu.add_command(label="(No recent files)", state=DISABLED)

    def _update_templates_menu(self):
        self.insert_template_menu.delete(0, END)
        templates = self.settings.get("templates", {})
        for name in sorted(templates.keys()):
            self.insert_template_menu.add_command(label=name, command=lambda n=name: self._insert_template(n))
        if not templates:
            self.insert_template_menu.add_command(label="(No templates)", state=DISABLED)

    def _create_connection_panel(self, parent):
        frame = tb.LabelFrame(parent, text="Connection", padding=10)
        frame.pack(fill=X, pady=(5, 10), padx=5)

        conn_grid = tb.Frame(frame)
        conn_grid.pack(fill=X)
        conn_grid.columnconfigure(1, weight=1)

        tb.Label(conn_grid, text="COM Port:").grid(row=0, column=0, sticky=W, pady=2)
        self.com_port_cb = tb.Combobox(conn_grid, state="readonly", width=12)
        self.com_port_cb.grid(row=0, column=1, sticky=EW, padx=5, pady=2)
        self.com_port_cb.bind("<<ComboboxSelected>>", lambda e: self.com_port_cb.selection_clear())
        self.com_port_cb.bind("<Button-1>", lambda e: self._update_com_ports(auto_select=False))

        self.refresh_btn = tb.Button(conn_grid, image=self.icons['refresh'], command=self._update_com_ports,
                                     bootstyle="secondary-outline")
        self.refresh_btn.grid(row=0, column=2, padx=(0, 5), pady=2)
        ToolTip(self.refresh_btn, text="Refresh COM port list.", bootstyle="info")

        tb.Label(conn_grid, text="Unit ID:").grid(row=1, column=0, sticky=W, pady=2)
        self.unit_id_entry = tb.Entry(conn_grid, width=15, validate="key",
                                      validatecommand=(self.register(self.validate_int), '%P'))
        self.unit_id_entry.insert(0, self.settings.get("unit_id", "30"))
        self.unit_id_entry.grid(row=1, column=1, columnspan=2, sticky=EW, padx=5, pady=2)

        btn_frame = tb.Frame(frame)
        btn_frame.pack(fill=X, pady=(10, 0))
        btn_frame.columnconfigure((0, 1), weight=1)
        self.connect_btn = tb.Button(btn_frame, text="Connect", command=self._connect_pump, bootstyle="success")
        self.connect_btn.grid(row=0, column=0, padx=(0, 5), sticky=EW)
        self.disconnect_btn = tb.Button(btn_frame, text="Disconnect", command=self._disconnect_pump,
                                        bootstyle="danger-outline")
        self.disconnect_btn.grid(row=0, column=1, padx=(5, 0), sticky=EW)

    def _create_execution_panel(self, parent):
        frame = tb.LabelFrame(parent, text="Sequence Execution", padding=10)
        frame.pack(fill=X, pady=10, padx=5)
        frame.columnconfigure((0, 1), weight=1)

        self.run_seq_btn = tb.Button(frame, text=" Run", image=self.icons['play'], compound=LEFT,
                                     command=self._run_sequence, bootstyle="success")
        self.run_seq_btn.grid(row=0, column=0, sticky=EW, ipady=5, pady=(5, 2), padx=(0, 2))

        self.pause_seq_btn = tb.Button(frame, text=" Pause", image=self.icons['pause'], compound=LEFT,
                                       command=self._pause_resume_sequence, bootstyle="warning")
        self.pause_seq_btn.grid(row=0, column=1, sticky=EW, ipady=5, pady=(5, 2), padx=(2, 0))

        self.stop_seq_btn = tb.Button(frame, text=" Stop Sequence", image=self.icons['stop'], compound=LEFT,
                                      command=self._stop_sequence, bootstyle="danger")
        self.stop_seq_btn.grid(row=1, column=0, columnspan=2, sticky=EW, ipady=5, pady=(2, 10))

        tb.Label(frame, text="Current Step Progress:").grid(row=2, column=0, columnspan=2, sticky=W, pady=(5, 0))
        self.step_progress = tb.Progressbar(frame, orient='horizontal', mode='determinate', bootstyle="success-striped")
        self.step_progress.grid(row=3, column=0, columnspan=2, sticky=EW, pady=(2, 5))

        self.step_time_label = tb.Label(frame, text="Step time: 00:00 / 00:00", font=self.small_font)
        self.step_time_label.grid(row=4, column=0, columnspan=2, sticky=E)

        tb.Label(frame, text="Total Progress:").grid(row=5, column=0, columnspan=2, sticky=W)
        self.total_progress = tb.Progressbar(frame, orient='horizontal', mode='determinate', bootstyle="info-striped")
        self.total_progress.grid(row=6, column=0, columnspan=2, sticky=EW, pady=(2, 5))

        info_frame = tb.Frame(frame)
        info_frame.grid(row=7, column=0, columnspan=2, sticky='ew')
        self.total_duration_label = tb.Label(info_frame, text="Total Duration: 00:00:00", font=self.small_font)
        self.total_duration_label.pack(side=LEFT)
        self.dyn_label = tb.Label(info_frame, text="Dyn: 0.00 dyn/cm²", font=self.small_font)
        self.dyn_label.pack(side=RIGHT)

    def _create_manual_control_panel(self, parent):
        self.manual_frame = CollapsibleFrame(parent, text="Manual Control", bootstyle="secondary")
        self.manual_frame.pack(fill=X, pady=10, padx=5)
        container = self.manual_frame.sub_frame
        container.columnconfigure(0, weight=1)

        self.speed_scale = tb.Scale(container, from_=0, to=48, orient=HORIZONTAL,
                                    command=self._update_speed_label_from_scale, bootstyle="info")
        self.speed_scale.grid(row=0, column=0, columnspan=2, sticky=EW, pady=(5, 2))

        speed_label_frame = tb.Frame(container)
        speed_label_frame.grid(row=1, column=0, columnspan=2, sticky=EW, pady=(0, 10))
        tb.Label(speed_label_frame, text="RPM:").pack(side=LEFT)
        self.vcmd_float_main = (self.register(self.validate_float), '%P')
        self.manual_rpm_entry = tb.Entry(speed_label_frame, width=6, validate="key",
                                         validatecommand=self.vcmd_float_main)
        self.manual_rpm_entry.pack(side=LEFT, padx=5)
        self.manual_rpm_entry.bind("<Return>", self._set_rpm_from_entry)
        ToolTip(self.manual_rpm_entry, "Type RPM (0-48) and press Enter.", bootstyle="info")

        btn_frame = tb.Frame(container)
        btn_frame.grid(row=2, column=0, columnspan=2, sticky=EW, pady=5)
        btn_frame.columnconfigure((0, 1), weight=1)
        self.fwd_btn = tb.Button(btn_frame, text="▶ Forward", command=self._manual_start_fwd,
                                 bootstyle="primary-outline")
        self.fwd_btn.grid(row=0, column=0, sticky=EW, padx=(0, 2))
        self.rev_btn = tb.Button(btn_frame, text="◀ Backward", command=self._manual_start_rev,
                                 bootstyle="primary-outline")
        self.rev_btn.grid(row=0, column=1, sticky=EW, padx=(2, 0))

        self.stop_btn = tb.Button(container, text="⏹️ STOP", command=self._manual_stop, bootstyle="danger")
        self.stop_btn.grid(row=3, column=0, columnspan=2, sticky=EW, pady=5, ipady=5)

    def _create_sequence_editor(self, parent):
        frame = tb.LabelFrame(parent, text="Sequence Editor", padding=10)
        frame.pack(fill=BOTH, expand=True, padx=(10, 0), pady=(5, 0))

        tree_frame = tb.Frame(frame)
        tree_frame.pack(fill=BOTH, expand=True, pady=5)
        cols = ("#", " ", "Type", "Details", "Duration")
        self.sequence_tree = tb.Treeview(tree_frame, columns=cols, show="headings",
                                         bootstyle="primary", style="Table.Treeview")
        for col, width, anchor in zip(cols, [40, 25, 80, 400, 200], [CENTER, CENTER, W, W, W]):
            self.sequence_tree.heading(col, text=col)
            self.sequence_tree.column(col, width=width, anchor=anchor, stretch=False if col != "Details" else True)
        self.sequence_tree.column(" ", stretch=False)  # Icon column

        vsb = tb.Scrollbar(tree_frame, orient=VERTICAL, command=self.sequence_tree.yview, bootstyle="round-primary")
        vsb.pack(side=RIGHT, fill=Y)
        self.sequence_tree.pack(side=LEFT, fill=BOTH, expand=True)
        self.sequence_tree.configure(yscrollcommand=vsb.set)

        # Color coding for directions
        self.sequence_tree.tag_configure('forward', background='#e6f4ea')
        self.sequence_tree.tag_configure('backward', background='#fdecea')
        self.sequence_tree.tag_configure('disabled', foreground='gray')

        self.sequence_tree.bind("<Double-1>", self._edit_item)
        self.sequence_tree.bind("<Button-3>", self._show_context_menu)
        self.sequence_tree.bind("<<TreeviewSelect>>", self._on_tree_select)

        controls_frame = tb.Frame(frame)
        controls_frame.pack(fill=X, pady=(10, 0))

        self.add_phase_btn = tb.Button(controls_frame, text=" Add Phase", image=self.icons['add'], compound=LEFT,
                                       command=self._add_phase, bootstyle="success")
        self.add_phase_btn.pack(side=LEFT, padx=(0, 2))
        self.add_cycle_btn = tb.Button(controls_frame, text=" Add Cycle", image=self.icons['cycle'], compound=LEFT,
                                       command=self._add_cycle, bootstyle="info")
        self.add_cycle_btn.pack(side=LEFT, padx=2)

        self.clear_btn = tb.Button(controls_frame, text=" Clear All", image=self.icons['clear'], compound=LEFT,
                                   command=self._clear_sequence, bootstyle="danger-outline")
        self.clear_btn.pack(side=RIGHT, padx=2)
        self.remove_btn = tb.Button(controls_frame, text=" Remove", image=self.icons['remove'], compound=LEFT,
                                    command=self._remove_item, bootstyle="danger-outline")
        self.remove_btn.pack(side=RIGHT, padx=2)
        self.down_btn = tb.Button(controls_frame, text=" Down", image=self.icons['down'], compound=LEFT,
                                  command=lambda: self._move_selected_item(1), bootstyle="secondary-outline")
        self.down_btn.pack(side=RIGHT, padx=2)
        self.up_btn = tb.Button(controls_frame, text=" Up", image=self.icons['up'], compound=LEFT,
                                command=lambda: self._move_selected_item(-1), bootstyle="secondary-outline")
        self.up_btn.pack(side=RIGHT, padx=(10, 2))

    def _create_plot_panel(self, parent):
        self.plot_pane = tb.PanedWindow(parent, orient=VERTICAL)
        self.plot_pane.pack(fill=BOTH, expand=True, padx=(10, 0), pady=(10, 0))

        plot_frame = tb.LabelFrame(self.plot_pane, text="Sequence Preview & Live Run", padding=(10, 5))
        self.plot_pane.add(plot_frame, weight=3)
        self.fig = Figure(figsize=(9, 2), dpi=100)
        self.ax = self.fig.add_subplot(111)
        self.canvas = FigureCanvasTkAgg(self.fig, master=plot_frame)
        self.canvas.get_tk_widget().pack(side=TOP, fill=BOTH, expand=True)
        self.fig.tight_layout(pad=0.5)

        plot_btn_frame = tb.Frame(plot_frame)
        plot_btn_frame.pack(fill=X, pady=(5, 0))

        tb.Button(plot_btn_frame, image=self.icons['export_img'], command=self._export_plot_image,
                  bootstyle="link").pack(side=RIGHT)
        ToolTip(plot_btn_frame.winfo_children()[-1], "Export chart as image")
        tb.Button(plot_btn_frame, image=self.icons['export_csv'], command=self._export_sequence_csv,
                  bootstyle="link").pack(side=RIGHT)
        ToolTip(plot_btn_frame.winfo_children()[-1], "Export sequence data as CSV")

        self.live_track_var = tk.BooleanVar(value=False)
        track_btn = tb.Checkbutton(plot_btn_frame, text="Live Track", variable=self.live_track_var,
                                   bootstyle="round-toggle", command=self._toggle_live_track)
        track_btn.pack(side=LEFT, padx=5)
        ToolTip(track_btn, "Toggle live tracking to focus on the current progress.")

    def _create_log_panel(self, parent):
        frame = tb.LabelFrame(parent, text="Log", padding=(10, 5))
        frame.pack(side=BOTTOM, fill=BOTH, expand=True, padx=5, pady=(0, 5))

        log_controls = tb.Frame(frame)
        log_controls.pack(fill=X)
        self.log_filter_cb = tb.Combobox(log_controls, values=["ALL", "INFO", "ERROR", "CONNECTION"],
                                         state="readonly", width=12)
        self.log_filter_cb.pack(side=LEFT)
        self.log_filter_cb.set("ALL")
        self.log_filter_cb.bind("<<ComboboxSelected>>", self._apply_log_filter)
        self.log_search_entry = tb.Entry(log_controls)
        self.log_search_entry.pack(side=LEFT, fill=X, expand=True, padx=5)
        self.log_search_entry.bind("<KeyRelease>", self._apply_log_filter)

        log_text_frame = tb.Frame(frame)
        log_text_frame.pack(fill=BOTH, expand=True, pady=(5, 0))
        self.log_text = tb.Text(log_text_frame, height=6, state=DISABLED, wrap=WORD, font=self.small_font)
        vsb = tb.Scrollbar(log_text_frame, orient=VERTICAL, command=self.log_text.yview, bootstyle="round-secondary")
        self.log_text.configure(yscrollcommand=vsb.set)
        vsb.pack(side=RIGHT, fill=Y)
        self.log_text.pack(side=LEFT, fill=BOTH, expand=True)

    def _create_status_bar(self):
        status_bar = tb.Frame(self, bootstyle="secondary")
        status_bar.pack(side=BOTTOM, fill=X)
        self.status_label = tb.Label(status_bar, text=" Status: Disconnected", image=self.icons['stop'], compound=LEFT,
                                     bootstyle="inverse-secondary", padding=5)
        self.status_label.pack(side=LEFT)
        self.status_filename = tb.Label(status_bar, text="New Sequence*", bootstyle="inverse-secondary", padding=5)
        self.status_filename.pack(side=LEFT, padx=10)
        self.status_info = tb.Label(status_bar, text="", bootstyle="inverse-secondary", padding=5, font=self.small_font)
        self.status_info.pack(side=RIGHT)

    def _show_context_menu(self, event):
        sel = self.sequence_tree.selection()
        if not sel: return
        idx = self.sequence_tree.index(sel[0])

        cm = tb.Menu(self, tearoff=0)
        cm.add_command(label=" Edit", image=self.icons['edit'], compound=LEFT, command=lambda: self._edit_item(event))
        cm.add_command(label=" Copy", image=self.icons['copy'], compound=LEFT, command=self._copy_item,
                       accelerator="Ctrl+C")
        cm.add_command(label=" Paste Below", image=self.icons['paste'], compound=LEFT, command=self._paste_item,
                       accelerator="Ctrl+V", state=NORMAL if self.clipboard else DISABLED)
        cm.add_command(label=" Duplicate", image=self.icons['duplicate'], compound=LEFT, command=self._duplicate_item,
                       accelerator="Ctrl+D")

        is_enabled = self.sequence_data[idx].get("enabled", True)
        toggle_text = "Disable" if is_enabled else "Enable"
        toggle_icon = self.icons['toggle_off'] if is_enabled else self.icons['toggle_on']
        cm.add_command(label=f" {toggle_text} Step", image=toggle_icon, compound=LEFT,
                       command=self._toggle_item_enabled)

        cm.add_separator()
        cm.add_command(label=" Move Up", image=self.icons['up'], compound=LEFT,
                       command=lambda: self._move_selected_item(-1), state=NORMAL if idx > 0 else DISABLED)
        cm.add_command(label=" Move Down", image=self.icons['down'], compound=LEFT,
                       command=lambda: self._move_selected_item(1),
                       state=NORMAL if idx < len(self.sequence_data) - 1 else DISABLED)
        cm.add_separator()
        cm.add_command(label=" Remove", image=self.icons['remove'], compound=LEFT, command=self._remove_item,
                       accelerator="Delete")
        cm.tk_popup(event.x_root, event.y_root)

    def _setup_shortcuts(self):
        self.bind_all("<Control-n>", self._clear_sequence)
        self.bind_all("<Control-o>", self._load_sequence_dialog)
        self.bind_all("<Control-s>", self._save_sequence)
        self.bind_all("<Control-Shift-S>", self._save_sequence_as)
        self.bind("<Delete>", self._remove_item)
        self.bind_all("<Control-c>", self._copy_item)
        self.bind_all("<Control-v>", self._paste_item)
        self.bind_all("<Control-d>", self._duplicate_item)

    # --- Validation Methods ---
    def validate_float(self, val):
        if val in ["", "-"]: return True
        try:
            float(val)
            return True
        except ValueError:
            return False

    def validate_int(self, val):
        if val in ["", "-"]: return True
        try:
            int(val)
            return True
        except ValueError:
            return False

    # --- UI Update & State Management ---
    def _set_theme(self, theme_name):
        """Applies a theme and updates all style-dependent widgets."""
        self.style.theme_use(theme_name)
        self.settings['theme'] = theme_name
        self._update_styles_and_widgets()

    def _open_settings(self):
        dialog = SettingsDialog(self, self.settings)
        if dialog.result:
            self.settings = dialog.result
            self.style.theme_use(self.settings.get("theme", "litera"))
            self._update_styles_and_widgets()
            self._update_templates_menu()
            self._schedule_autosave()
            self._update_total_duration()
            self._log("Settings updated.", "INFO")

    def _update_ui_states(self):
        is_seq_running = self.is_running_sequence

        # Connection Panel
        conn_state = DISABLED if is_seq_running else NORMAL
        self.connect_btn.config(state=DISABLED if self.is_connected or is_seq_running else NORMAL)
        self.disconnect_btn.config(state=NORMAL if self.is_connected and not is_seq_running else DISABLED)
        self.com_port_cb.config(state="readonly" if conn_state == NORMAL else DISABLED)
        self.unit_id_entry.config(state=conn_state)
        self.refresh_btn.config(state=conn_state)

        # Execution Panel
        can_run = self.is_connected and not is_seq_running and any(s.get("enabled", True) for s in self.sequence_data)
        self.run_seq_btn.config(state=NORMAL if can_run else DISABLED)
        self.pause_seq_btn.config(state=NORMAL if is_seq_running else DISABLED)
        self.stop_seq_btn.config(state=NORMAL if is_seq_running else DISABLED)

        # Manual Control
        manual_state = NORMAL if self.is_connected and not is_seq_running else DISABLED
        for w in self.manual_frame.sub_frame.winfo_children():
            try:
                if isinstance(w, (tb.Frame)):
                    for grandchild in w.winfo_children(): grandchild.config(state=manual_state)
                else:
                    w.config(state=manual_state)
            except tk.TclError:
                pass

        # Sequence Editor Buttons
        editor_state = NORMAL if not is_seq_running else DISABLED
        self.add_phase_btn.config(state=editor_state)
        self.add_cycle_btn.config(state=editor_state)
        self.clear_btn.config(state=editor_state)
        self._on_tree_select()  # Update selection-based buttons

    def _on_tree_select(self, event=None):
        """Updates UI based on Treeview selection."""
        is_seq_running = self.is_running_sequence
        has_selection = bool(self.sequence_tree.selection())
        editor_state = NORMAL if not is_seq_running and has_selection else DISABLED

        self.remove_btn.config(state=editor_state)
        self.up_btn.config(state=editor_state)
        self.down_btn.config(state=editor_state)

    @contextlib.contextmanager
    def _busy_cursor(self):
        self.config(cursor="watch")
        self.update_idletasks()
        try:
            yield
        finally:
            self.config(cursor="")

    def _update_status_bar(self, status_text=None, filename_text=None, info_text=None):
        if status_text is not None: self.status_label.config(text=status_text)
        if filename_text is not None: self.status_filename.config(text=filename_text)
        if info_text is not None: self.status_info.config(text=info_text)

    # --- Backend Communication ---
    def _send_pump_command(self, command_str):
        self.command_queue.put({"action": "send_command", "command_str": command_str})

    def _process_results(self):
        try:
            while not self.result_queue.empty():
                result = self.result_queue.get_nowait()
                status, msg = result.get("status"), result.get("msg")

                log_level = "INFO"
                if status == "error":
                    log_level = "ERROR"
                    messagebox.showerror("Controller Error", msg)
                elif status in ["connected", "disconnected"]:
                    log_level = "CONNECTION"

                if status != "log": self._log(msg, log_level)

                if status == "connected":
                    self.is_connected = True
                    self.status_label.config(text=" Status: Connected", image=self.icons['play'],
                                             bootstyle="inverse-success")
                    self.settings['last_com_port'] = self.com_port_cb.get()
                    self.settings['unit_id'] = self.unit_id_entry.get()
                    self._send_pump_command("SR")
                elif status == "disconnected":
                    self.is_connected = False
                    self.is_running_sequence = False  # Force stop if disconnected
                    self.status_label.config(text=" Status: Disconnected", image=self.icons['stop'],
                                             bootstyle="inverse-secondary")
                self._update_ui_states()
        finally:
            self.after(100, self._process_results)

    def _log(self, message, level="INFO"):
        """ Logs a message with a level for filtering. """
        timestamp = time.strftime('%H:%M:%S')
        log_entry = f"{timestamp} [{level}] - {message}\n"

        # This internal list stores all logs
        if not hasattr(self, '_all_logs'):
            self._all_logs = []
        self._all_logs.append(log_entry)

        self._apply_log_filter()

    def _apply_log_filter(self, event=None):
        """ Filters and displays logs in the text widget. """
        if not hasattr(self, 'log_text') or not self.log_text.winfo_exists(): return
        if not hasattr(self, '_all_logs'): return

        filt = self.log_filter_cb.get()
        search_term = self.log_search_entry.get().lower()

        self.log_text.config(state=NORMAL)
        self.log_text.delete(1.0, END)

        for entry in self._all_logs:
            display = False
            if (filt == "ALL" or f"[{filt}]" in entry):
                if not search_term or search_term in entry.lower():
                    display = True

            if display:
                level_tag = "normal"
                if "[ERROR]" in entry:
                    level_tag = "error"
                elif "[CONNECTION]" in entry:
                    level_tag = "connection"

                self.log_text.tag_config("error", foreground=self.style.colors.danger)
                self.log_text.tag_config("connection", foreground=self.style.colors.info)

                self.log_text.insert(END, entry, (level_tag,))

        self.log_text.config(state=DISABLED)
        self.log_text.see(END)

    # --- Widget Logic & Callbacks ---
    def _update_com_ports(self, auto_select=False):
        with self._busy_cursor():
            ports = [port.device for port in serial.tools.list_ports.comports()]
            self.com_port_cb['values'] = ports
            last_port = self.settings.get("last_com_port")

            if auto_select:
                if last_port and last_port in ports:
                    self.com_port_cb.set(last_port)
                elif ports:
                    self.com_port_cb.set(ports[0])

    def _connect_pump(self):
        port = self.com_port_cb.get()
        if not port: messagebox.showerror("Connection Error", "No COM port selected."); return
        try:
            unit_id = int(self.unit_id_entry.get())
        except ValueError:
            messagebox.showerror("Input Error", "Unit ID must be an integer.");
            return

        with self._busy_cursor():
            self.status_label.config(text=" Status: Connecting...", image=self.icons['refresh'],
                                     bootstyle="inverse-info")
            self.update_idletasks()
            self.command_queue.put({"action": "connect", "port": port, "unit_id": unit_id, "baudrate": 19200})

    def _disconnect_pump(self):
        self._send_pump_command("SK")
        self.command_queue.put({"action": "disconnect"})

    def _update_speed_label_from_scale(self, value):
        rpm = float(value)
        self.manual_rpm_entry.delete(0, END)
        self.manual_rpm_entry.insert(0, f"{rpm:.1f}")

    def _set_rpm_from_entry(self, event=None):
        try:
            rpm = float(self.manual_rpm_entry.get())
            if not (0 <= rpm <= 48):
                messagebox.showwarning("Invalid RPM", "Please enter a value between 0 and 48.")
                return
            self.speed_scale.set(rpm)
        except (ValueError, tk.TclError):
            messagebox.showwarning("Invalid Input", "Please enter a valid number for RPM.")

    def _manual_start_fwd(self):
        self._set_rpm_from_entry()
        self._send_pump_command(f"R{int(self.speed_scale.get() * 100):03}")
        self._send_pump_command("K>")
        self.fwd_btn.config(bootstyle="primary")  # Visual feedback
        self.rev_btn.config(bootstyle="primary-outline")

    def _manual_start_rev(self):
        self._set_rpm_from_entry()
        self._send_pump_command(f"R{int(self.speed_scale.get() * 100):03}")
        self._send_pump_command("K<")
        self.rev_btn.config(bootstyle="primary")
        self.fwd_btn.config(bootstyle="primary-outline")

    def _manual_stop(self):
        self._send_pump_command("KH")
        self.fwd_btn.config(bootstyle="primary-outline")
        self.rev_btn.config(bootstyle="primary-outline")

    # --- Sequence Editor Logic ---
    def _add_phase(self):
        d = AddPhaseDialog(self)
        if d.result:
            self.sequence_data.append(d.result)
            self._mark_dirty(True)
            self._update_treeview()

    def _add_cycle(self):
        phases = [i + 1 for i, s in enumerate(self.sequence_data) if s['type'] == 'Phase']
        d = AddCycleDialog(self, phases)
        if d.result:
            self.sequence_data.append(d.result)
            self._mark_dirty(True)
            self._update_treeview()

    def _edit_item(self, event=None):
        sel = self.sequence_tree.selection()
        if not sel: return
        # Prevent editing while running
        if self.is_running_sequence:
            self._log("Cannot edit sequence while it is running.", "INFO")
            return
        idx = self.sequence_tree.index(sel[0])
        data = self.sequence_data[idx]

        dialog = None
        if data['type'] == 'Phase':
            dialog = AddPhaseDialog(self, data)
        elif data['type'] == 'Cycle':
            phases = [i + 1 for i, s in enumerate(self.sequence_data) if s['type'] == 'Phase']
            dialog = AddCycleDialog(self, phases, data)

        if dialog and dialog.result:
            self.sequence_data[idx] = dialog.result
            self._mark_dirty(True)
            self._update_treeview()
            self.sequence_tree.selection_set(self.sequence_tree.get_children()[idx])

    def _remove_item(self, event=None):
        sel = self.sequence_tree.selection()
        if not sel: return

        if messagebox.askyesno("Confirm Removal", f"Are you sure you want to remove {len(sel)} selected step(s)?"):
            indices = sorted([self.sequence_tree.index(i) for i in sel], reverse=True)
            for i in indices:
                del self.sequence_data[i]
            self._mark_dirty(True)
            self._update_treeview()

    def _clear_sequence(self, event=None):
        if self.is_dirty and not self._confirm_unsaved_changes(): return
        self.sequence_data.clear()
        self.current_filepath = None
        self._mark_dirty(False)
        self._update_treeview()

    def _move_selected_item(self, direction):
        sel = self.sequence_tree.selection()
        if not sel: return

        indices = [self.sequence_tree.index(i) for i in sel]
        if direction == -1:
            indices.sort()
            if indices[0] == 0: return
            for i in indices:
                self.sequence_data.insert(i - 1, self.sequence_data.pop(i))
            new_selection_indices = [i - 1 for i in indices]
        else:
            indices.sort(reverse=True)
            if indices[0] >= len(self.sequence_data) - 1: return
            for i in indices:
                self.sequence_data.insert(i + 1, self.sequence_data.pop(i))
            new_selection_indices = [i + 1 for i in indices]

        self._mark_dirty(True)
        self._update_treeview()

        new_iids = [self.sequence_tree.get_children()[i] for i in new_selection_indices]
        self.sequence_tree.selection_set(new_iids)
        if new_iids: self.sequence_tree.focus(new_iids[0])

    def _copy_item(self, event=None):
        sel = self.sequence_tree.selection()
        if not sel: return
        self.clipboard = [copy.deepcopy(self.sequence_data[self.sequence_tree.index(i)]) for i in sel]
        self._log(f"Copied {len(self.clipboard)} step(s).", "INFO")
        self._update_status_bar(info_text=f"Copied {len(self.clipboard)} step(s)")

    def _paste_item(self, event=None):
        if not self.clipboard: return
        sel = self.sequence_tree.selection()
        index = self.sequence_tree.index(sel[0]) if sel else len(self.sequence_data) - 1

        for item in reversed(self.clipboard):
            self.sequence_data.insert(index + 1, copy.deepcopy(item))
        self._mark_dirty(True)
        self._update_treeview()
        self._update_status_bar(info_text=f"Pasted {len(self.clipboard)} step(s)")

    def _duplicate_item(self, event=None):
        sel = self.sequence_tree.selection()
        if not sel: return
        self._copy_item()
        self._paste_item()

    def _toggle_item_enabled(self, event=None):
        sel = self.sequence_tree.selection()
        if not sel: return

        indices = [self.sequence_tree.index(i) for i in sel]
        # Determine the new state from the first selected item
        new_state = not self.sequence_data[indices[0]].get("enabled", True)

        for i in indices:
            self.sequence_data[i]["enabled"] = new_state

        self._mark_dirty(True)
        self._update_treeview()

    def _batch_edit_items(self):
        sel = self.sequence_tree.selection()
        phase_indices = [self.sequence_tree.index(i) for i in sel if
                         self.sequence_data[self.sequence_tree.index(i)]['type'] == 'Phase']

        if not phase_indices:
            messagebox.showinfo("No Phases Selected", "Please select one or more Phase steps to batch edit.")
            return

        dialog = BatchEditDialog(self, len(phase_indices))
        if dialog.result:
            for idx in phase_indices:
                for key, value in dialog.result.items():
                    self.sequence_data[idx][key] = value
            self._mark_dirty(True)
            self._update_treeview()
            self._log(f"Batch edited {len(phase_indices)} phases.", "INFO")

    def _update_treeview(self):
        selected_iids = self.sequence_tree.selection()
        self.sequence_tree.delete(*self.sequence_tree.get_children())

        for i, step in enumerate(self.sequence_data):
            details, duration = "", "N/A"
            icon = ""
            is_enabled = step.get("enabled", True)

            if step['type'] == 'Phase':
                icon = "▶" if step['direction'] == 'Forward' else "◀"
                details = f"{step['mode']} to {step['rpm']:.2f} RPM"
                if step['mode'] == 'Ramp': details += f" (interval: {step.get('update_interval', 1.0)}s)"
                duration = f"{step['duration']} {step['unit']}"
            elif step['type'] == 'Cycle':
                icon = "↻"
                details = f"Loop Phases {step['start_phase']}-{step['end_phase']} ({step['repeats']} times)"

            tags = []
            if step['type'] == 'Phase':
                tags.append('forward' if step['direction'] == 'Forward' else 'backward')
            if not is_enabled:
                tags.append('disabled')

            self.sequence_tree.insert(
                "",
                END,
                iid=i,
                values=(i + 1, icon, step['type'], details, duration),
                tags=tuple(tags),
            )

        # Restore selection
        for iid in selected_iids:
            if self.sequence_tree.exists(iid):
                self.sequence_tree.selection_add(iid)

        self._update_total_duration()
        if not self.is_running_sequence:
            self._update_plot()
        self._update_ui_states()

    def _update_total_duration(self):
        total_s = self._calculate_total_duration(include_disabled=False)
        duration_str = str(timedelta(seconds=int(total_s)))
        self.total_duration_label.config(text=f"Total Duration: {duration_str}")

        # Update shear stress estimation
        try:
            flat_sequence = self._flatten_sequence_for_plot(include_disabled=False)
            total_weighted_rpm = 0.0
            for step in flat_sequence:
                duration_s = step['duration'] * ({'s': 1, 'min': 60, 'hr': 3600}.get(step['unit'], 1))
                total_weighted_rpm += step['rpm'] * duration_s
            avg_rpm = total_weighted_rpm / total_s if total_s > 0 else 0

            eta = float(self.settings.get("dynamic_viscosity", DEFAULT_VISCOSITY))
            p_const = float(self.settings.get(
                "chamber_p_value",
                CHAMBER_COEFFICIENTS.get(self.settings.get("chamber_type", DEFAULT_CHAMBER), 176.1),
            ))
            k_coeff = float(self.settings.get("tube_coefficient", DEFAULT_TUBE_COEFFICIENT))
            tau = eta * p_const * (k_coeff * avg_rpm)
            self.dyn_label.config(text=f"Dyn: {tau:.2f} dyn/cm²")
        except Exception:
            self.dyn_label.config(text="Dyn: N/A")

    def _mark_dirty(self, dirty_state):
        self.is_dirty = dirty_state
        filename = os.path.basename(self.current_filepath) if self.current_filepath else "New Sequence"
        suffix = "*" if self.is_dirty else ""
        self._update_status_bar(filename_text=f"{filename}{suffix}")

    # --- Plotting & Exporting ---
    def _toggle_live_track(self):
        """Redraws the plot to apply the new live tracking view state."""
        if self.is_running_sequence:
            # During a run, the view will update on the next tick.
            # This just ensures the static view is correct if toggled off.
            if not self.live_track_var.get():
                self._update_plot(as_plan_background=True)
        else:
            self._update_plot()

    def _update_plot(self, as_plan_background=False):
        if not hasattr(self, 'ax'): return
        self.ax.clear()
        colors = self.style.colors
        fwd_color, rev_color = colors.info, colors.danger

        # Plot disabled steps in the background
        try:
            disabled_flat_seq = self._flatten_sequence_for_plot(include_disabled=True, only_disabled=True)
            current_time, current_rpm = 0.0, 0.0
            for step in disabled_flat_seq:
                duration_s = step['duration'] * ({'s': 1, 'min': 60, 'hr': 3600}.get(step['unit'], 1))
                end_t, end_rpm = current_time + duration_s, step['rpm']
                self.ax.plot([current_time, end_t], [step['rpm'], end_rpm], color='gray', linestyle=':', linewidth=1)
                current_time, current_rpm = end_t, end_rpm
        except RecursionError:
            pass  # Ignore if disabled part has loops

        # Plot enabled steps
        if as_plan_background:
            plan_color, plan_style = colors.secondary, '--'
        else:
            self.ax.plot([], [], color=fwd_color, label='Forward', linewidth=2)
            self.ax.plot([], [], color=rev_color, label='Backward', linewidth=2)
            plan_style = '-'

        current_time, current_rpm = 0.0, 0.0
        try:
            flat_sequence = self._flatten_sequence_for_plot(include_disabled=False)
            for step in flat_sequence:
                duration_s = step['duration'] * ({'s': 1, 'min': 60, 'hr': 3600}.get(step['unit'], 1))
                plot_color = fwd_color if step['direction'] == 'Forward' else rev_color
                if as_plan_background: plot_color = plan_color

                start_t, end_t = current_time, current_time + duration_s
                start_rpm, end_rpm = current_rpm, step['rpm']

                if step['mode'] == 'Fixed':
                    self.ax.plot([start_t, start_t], [start_rpm, end_rpm], color=plot_color, linestyle=':',
                                 linewidth=1.5)
                    self.ax.plot([start_t, end_t], [end_rpm, end_rpm], color=plot_color, linestyle=plan_style,
                                 linewidth=2)
                elif step['mode'] == 'Ramp':
                    self.ax.plot([start_t, end_t], [start_rpm, end_rpm], color=plot_color, linestyle=plan_style,
                                 linewidth=2)

                current_rpm, current_time = end_rpm, end_t
        except RecursionError:
            self.ax.text(0.5, 0.5, 'Error: Infinite loop in sequence.', transform=self.ax.transAxes, color='red',
                         ha='center', va='center')

        self.ax.set_xlabel("Time (s)");
        self.ax.set_ylabel("RPM")
        self.ax.set_title("Sequence RPM Profile" if not as_plan_background else "Sequence Live Run")
        if not as_plan_background and (self.sequence_data or self.plot_is_live):
            legend = self.ax.legend(prop={'size': self.small_font.cget("size")})
            for text in legend.get_texts(): text.set_color(colors.fg)
            frame = legend.get_frame();
            frame.set_facecolor(colors.bg);
            frame.set_edgecolor(colors.fg)

        self.fig.tight_layout(pad=1.0, h_pad=0.5, w_pad=0.5)
        self.canvas.draw()
        self._update_plot_style()

    def _update_actual_plot(self, time_pos, rpm_pos, direction):
        if not hasattr(self, 'ax') or not self.plot_is_live: return
        colors = self.style.colors
        color = colors.get('success')

        if direction != self.last_plot_direction:
            self.last_plot_direction = direction
            self.actual_plot_data = {"time": [time_pos], "rpm": [rpm_pos]}
            self.actual_plot_line, = self.ax.plot(self.actual_plot_data["time"], self.actual_plot_data["rpm"],
                                                  color=color, linewidth=2.5, alpha=0.8, label="Actual")
        else:
            self.actual_plot_data["time"].append(time_pos)
            self.actual_plot_data["rpm"].append(rpm_pos)
            self.actual_plot_line.set_data(self.actual_plot_data["time"], self.actual_plot_data["rpm"])

        if self.live_track_var.get():
            window_seconds = 60
            self.ax.set_xlim(max(0, time_pos - window_seconds), time_pos + 10)
            self.ax.relim()
            self.ax.autoscale_view(scalex=False, scaley=True)
        else:
            self.ax.relim()
            self.ax.autoscale_view()

        self.canvas.draw_idle()

    def _export_plot_image(self):
        if not hasattr(self, 'fig'): return
        fp = filedialog.asksaveasfilename(
            defaultextension=".png",
            filetypes=[("PNG Image", "*.png"), ("JPEG Image", "*.jpg"), ("SVG Vector", "*.svg")],
            title="Save Plot as Image"
        )
        if not fp: return
        try:
            self.fig.savefig(fp, dpi=300, facecolor=self.fig.get_facecolor())
            self._log(f"Plot saved to {fp}", "INFO")
        except Exception as e:
            messagebox.showerror("Export Failed", f"Could not save image: {e}")

    def _export_sequence_csv(self):
        if not self.sequence_data:
            messagebox.showinfo("No Data", "The sequence is empty.")
            return

        fp = filedialog.asksaveasfilename(
            defaultextension=".csv",
            filetypes=[("CSV File", "*.csv"), ("All Files", "*.*")],
            title="Save Sequence as CSV"
        )
        if not fp: return

        try:
            flat_sequence = self._flatten_sequence_for_plot(include_disabled=True)
            with open(fp, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(['Time (s)', 'Target RPM', 'Direction', 'Mode', 'Enabled'])

                current_time, current_rpm = 0.0, 0.0
                for step in flat_sequence:
                    duration_s = step['duration'] * ({'s': 1, 'min': 60, 'hr': 3600}.get(step['unit'], 1))
                    start_t, end_t = current_time, current_time + duration_s
                    start_rpm, end_rpm = current_rpm, step['rpm']

                    if step['mode'] == 'Ramp':
                        writer.writerow([f"{start_t:.2f}", f"{start_rpm:.2f}", step['direction'], "Ramp Start",
                                         step.get("enabled", True)])
                        writer.writerow([f"{end_t:.2f}", f"{end_rpm:.2f}", step['direction'], "Ramp End",
                                         step.get("enabled", True)])
                    else:  # Fixed
                        writer.writerow(
                            [f"{start_t:.2f}", f"{end_rpm:.2f}", step['direction'], "Fixed", step.get("enabled", True)])

                    current_time, current_rpm = end_t, end_rpm
            self._log(f"Sequence data exported to {fp}", "INFO")
        except Exception as e:
            messagebox.showerror("Export Failed", f"Could not export CSV data: {e}")

    def _flatten_sequence_for_plot(self, include_disabled=False, only_disabled=False):
        flat_list, pc, iterations, cycle_counters, MAX_ITER = [], 0, 0, {}, 10000
        while pc < len(self.sequence_data) and iterations < MAX_ITER:
            instr = self.sequence_data[pc]
            is_instr_enabled = instr.get("enabled", True)

            if only_disabled and is_instr_enabled:
                pc += 1
                continue
            if not include_disabled and not is_instr_enabled:
                pc += 1
                continue

            if instr['type'] == 'Phase':
                flat_list.append(instr)
                pc += 1
            elif instr['type'] == 'Cycle':
                if is_instr_enabled:
                    if pc not in cycle_counters: cycle_counters[pc] = instr['repeats']
                    if cycle_counters[pc] > 0:
                        cycle_counters[pc] -= 1
                        target_pc = instr['start_phase'] - 1
                        if 0 <= target_pc < len(self.sequence_data):
                            pc = target_pc
                        else:
                            self._log(f"Error: Invalid start phase #{instr['start_phase']} in cycle.", "ERROR");
                            break
                    else:
                        del cycle_counters[pc];
                        pc += 1
                else:  # Disabled cycle, just skip it
                    pc += 1
            iterations += 1
        if iterations >= MAX_ITER:
            self._log("Error: Sequence has too many steps or an infinite loop.", "ERROR")
            raise RecursionError("Sequence flattening limit reached")
        return flat_list

    # --- Sequence Execution ---
    def _run_sequence(self):
        if not any(s.get("enabled", True) for s in self.sequence_data):
            messagebox.showwarning("Warning", "Sequence is empty or all steps are disabled.")
            return

        self.is_running_sequence = True
        self.is_paused = False
        self._update_ui_states()
        self._log("Starting sequence...", "INFO")

        self.plot_is_live = True
        self._update_plot(as_plan_background=True)
        self.actual_plot_data = {"time": [0], "rpm": [0]}
        self.last_plot_direction = None

        self.stop_event.clear()
        self.pause_event.clear()
        self.sequence_thread = threading.Thread(target=self._sequence_worker, daemon=True)
        self.sequence_thread.start()

    def _sequence_worker(self):
        self.current_rpm = 0.0
        pc, cycle_counters = 0, {}
        cumulative_time = 0.0

        while pc < len(self.sequence_data) and not self.stop_event.is_set():
            if self.pause_event.is_set():
                time.sleep(0.2)
                continue  # Loop here while paused

            # Find next enabled step
            while pc < len(self.sequence_data) and not self.sequence_data[pc].get("enabled", True):
                pc += 1
            if pc >= len(self.sequence_data): break

            if pc < len(self.sequence_tree.get_children()):
                self.after(0, lambda p=pc: (
                    self.sequence_tree.selection_set(self.sequence_tree.get_children()[p]),
                    self.sequence_tree.focus(self.sequence_tree.get_children()[p]),
                    self.sequence_tree.see(self.sequence_tree.get_children()[p])
                ))

            instruction = self.sequence_data[pc]
            if instruction['type'] == 'Phase':
                phase_start_time = cumulative_time
                for time_in_phase, rpm_in_phase, direction in self._execute_phase(instruction):
                    if self.stop_event.is_set(): break
                    pause_start_time = time.time()
                    while self.pause_event.is_set(): time.sleep(0.1)
                    if self.is_paused:
                        phase_start_time += time.time() - pause_start_time

                    cumulative_time = phase_start_time + time_in_phase
                    self.after(0, self._update_actual_plot, cumulative_time, rpm_in_phase, direction)
                    self.current_rpm = rpm_in_phase
                if self.stop_event.is_set(): break
                duration_s = instruction['duration'] * ({'s': 1, 'min': 60, 'hr': 3600}.get(instruction['unit'], 1))
                cumulative_time = phase_start_time + duration_s
                pc += 1
            elif instruction['type'] == 'Cycle':
                if pc not in cycle_counters: cycle_counters[pc] = instruction['repeats']
                if cycle_counters[pc] > 0:
                    cycle_counters[pc] -= 1
                    pc = instruction['start_phase'] - 1
                else:
                    del cycle_counters[pc];
                    pc += 1

        if not self.stop_event.is_set():
            self._log("Sequence finished. Stopping pump.", "INFO")
            self._send_pump_command("KH")
        self.after(0, self._on_sequence_finish)

    def _execute_phase(self, phase):
        direction, mode = phase['direction'], phase['mode']
        self._send_pump_command("K>" if direction == 'Forward' else "K<")
        duration_s = phase['duration'] * ({'s': 1, 'min': 60, 'hr': 3600}.get(phase['unit'], 1))

        PROGRESS_INTERVAL, elapsed = 0.1, 0.0
        total_duration_global = self._calculate_total_duration(include_disabled=False)
        time_before_phase = self.actual_plot_data["time"][-1] if self.actual_plot_data["time"] else 0

        start_rpm = self.current_rpm
        if mode == 'Fixed':
            self._send_pump_command(f"R{int(phase['rpm'] * 100):03}")

        while elapsed < duration_s and not self.stop_event.is_set():
            pause_start = time.time()
            self.stop_event.wait(PROGRESS_INTERVAL)
            if self.pause_event.is_set():
                time_before_phase += time.time() - pause_start
                continue

            elapsed += PROGRESS_INTERVAL

            if mode == 'Fixed':
                current_rpm_in_phase = phase['rpm']
            elif mode == 'Ramp':
                update_interval = phase.get('update_interval', 1.0)
                progress_in_phase = elapsed / duration_s if duration_s > 0 else 1
                current_rpm_in_phase = start_rpm + (phase['rpm'] - start_rpm) * progress_in_phase
                if int(elapsed / update_interval) > int((elapsed - PROGRESS_INTERVAL) / update_interval):
                    self._send_pump_command(f"R{int(current_rpm_in_phase * 100):03}")

            yield elapsed, current_rpm_in_phase, direction
            self._update_progress_bars(elapsed, duration_s, time_before_phase, total_duration_global)

        if not self.stop_event.is_set(): self._send_pump_command(f"R{int(phase['rpm'] * 100):03}")

    def _update_progress_bars(self, elapsed_step, duration_step, time_before, duration_total):
        step_prog = (elapsed_step / duration_step) * 100 if duration_step > 0 else 100
        total_prog = ((time_before + elapsed_step) / duration_total) * 100 if duration_total > 0 else 100

        elapsed_str = str(timedelta(seconds=int(elapsed_step)))
        duration_str = str(timedelta(seconds=int(duration_step)))

        def _update():
            if not self.winfo_exists(): return
            self.step_progress.config(value=step_prog)
            self.total_progress.config(value=total_prog)
            self.step_time_label.config(text=f"Step time: {elapsed_str} / {duration_str}")

        self.after(0, _update)

    def _calculate_total_duration(self, include_disabled=False):
        try:
            return sum(s['duration'] * ({'s': 1, 'min': 60, 'hr': 3600}.get(s['unit'], 1)) for s in
                       self._flatten_sequence_for_plot(include_disabled=include_disabled))
        except RecursionError:
            return 0

    def _on_sequence_finish(self):
        self.is_running_sequence = False
        self.is_paused = False
        self.plot_is_live = False
        self.step_progress.config(value=0)
        self.total_progress.config(value=0)
        self.after(100, self._update_plot)
        self._update_ui_states()
        self.pause_seq_btn.config(text=" Pause", image=self.icons['pause'])
        if self.stop_event.is_set():
            self._log("Sequence execution was stopped by user.", "INFO")

    def _pause_resume_sequence(self):
        self.is_paused = not self.is_paused
        if self.is_paused:
            self.pause_event.set()
            self._send_pump_command("KH")  # Halt pump on pause
            self.pause_seq_btn.config(text=" Resume", image=self.icons['play'])
            self._log("Sequence paused.", "INFO")
        else:
            self.pause_event.clear()
            self.pause_seq_btn.config(text=" Pause", image=self.icons['pause'])
            self._log("Sequence resumed.", "INFO")

    def _stop_sequence(self):
        if self.sequence_thread and self.sequence_thread.is_alive():
            self._log("STOP pressed. Halting pump and sequence...", "INFO")
            self._send_pump_command("KH")
            self.stop_event.set()
            self.pause_event.clear()

    # --- File I/O & Templates ---
    def _confirm_unsaved_changes(self):
        if self.is_dirty:
            res = messagebox.askyesnocancel("Unsaved Changes", "You have unsaved changes. Do you want to save them?")
            if res is True:
                return self._save_sequence()
            elif res is None:
                return False
        return True

    def _load_sequence_dialog(self, event=None):
        if not self._confirm_unsaved_changes(): return
        fp = filedialog.askopenfilename(filetypes=[("JSON Sequence", "*.json"), ("All Files", "*.*")])
        if fp:
            self._load_sequence(fp)

    def _load_sequence(self, filepath):
        if not self._confirm_unsaved_changes(): return
        try:
            with self._busy_cursor(), open(filepath, 'r', encoding='utf-8') as f:
                self.sequence_data = json.load(f)
            self.current_filepath = filepath
            self._update_treeview()
            self._log(f"Sequence loaded from: {filepath}", "INFO")
            self._mark_dirty(False)
            self._add_to_recent_files(filepath)
        except Exception as e:
            messagebox.showerror("Load Failed", f"Could not load or parse file: {e}")
            if filepath in self.settings.get('recent_files', []):
                self.settings['recent_files'].remove(filepath)
                self._update_recent_files_menu()

    def _save_sequence(self, event=None):
        if not self.current_filepath:
            return self._save_sequence_as()
        try:
            with self._busy_cursor(), open(self.current_filepath, 'w', encoding='utf-8') as f:
                json.dump(self.sequence_data, f, indent=4)
            self._log(f"Sequence saved to: {self.current_filepath}", "INFO")
            self._mark_dirty(False)
            self._add_to_recent_files(self.current_filepath)
            return True
        except Exception as e:
            messagebox.showerror("Save Failed", f"Could not save file: {e}")
            return False

    def _save_sequence_as(self, event=None):
        fp = filedialog.asksaveasfilename(defaultextension=".json",
                                          filetypes=[("JSON Sequence", "*.json"), ("All Files", "*.*")])
        if not fp: return False
        self.current_filepath = fp
        return self._save_sequence()

    def _save_selection_as_template(self):
        sel = self.sequence_tree.selection()
        if not sel:
            messagebox.showinfo("No Selection", "Please select one or more steps to save as a template.")
            return

        from tkinter.simpledialog import askstring
        template_name = askstring("Template Name", "Enter a name for this template:", parent=self)
        if not template_name: return

        if template_name in self.settings.get("templates", {}):
            if not messagebox.askyesno("Overwrite?", f"Template '{template_name}' already exists. Overwrite?"):
                return

        template_data = [copy.deepcopy(self.sequence_data[self.sequence_tree.index(i)]) for i in sel]
        if "templates" not in self.settings:
            self.settings["templates"] = {}
        self.settings["templates"][template_name] = template_data

        self._update_templates_menu()
        self._log(f"Saved selection as template '{template_name}'.", "INFO")

    def _insert_template(self, name):
        template_data = self.settings.get("templates", {}).get(name)
        if not template_data: return

        sel = self.sequence_tree.selection()
        index = self.sequence_tree.index(sel[0]) if sel else len(self.sequence_data) - 1

        for item in reversed(template_data):
            self.sequence_data.insert(index + 1, copy.deepcopy(item))

        self._mark_dirty(True)
        self._update_treeview()
        self._log(f"Inserted template '{name}'.", "INFO")

    # --- Application Shutdown ---
    def _on_closing(self):
        if not self._confirm_unsaved_changes():
            return

        if self.is_connected: self._disconnect_pump()
        self.command_queue.put({"action": "stop_thread"})
        self.stop_event.set()

        if self.sequence_thread: self.sequence_thread.join(timeout=0.2)
        self.controller_thread.join(timeout=0.5)

        if self.autosave_timer_id: self.after_cancel(self.autosave_timer_id)
        # Clean up backup file on successful exit
        with contextlib.suppress(OSError):
            if os.path.exists(BACKUP_FILE):
                os.remove(BACKUP_FILE)

        self._save_settings()
        self.destroy()


if __name__ == "__main__":
    try:
        app = PumpControlUI()
        app.mainloop()
    except Exception as e:
        import traceback

        error_msg = f"A fatal error occurred and the application must close.\n\n{traceback.format_exc()}"
        messagebox.showerror("Fatal Error", error_msg)
        with open("fatal_error_log.txt", "w") as f:
            f.write(error_msg)
