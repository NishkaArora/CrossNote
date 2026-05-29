// Returns the label to apply to a post, or null if the post should not be labeled.
export function detectLabel(text: string): "misinformation" | null {
  if (text.toLowerCase().includes("crossnote")) {
    return "misinformation";
  }
  return null;
}
