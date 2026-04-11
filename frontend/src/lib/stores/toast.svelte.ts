/** Toast notification store. */

export interface ToastMessage {
  id: number;
  text: string;
  type: "info" | "success" | "error";
}

let nextId = 0;

function createToastStore() {
  let toasts = $state<ToastMessage[]>([]);

  return {
    get toasts() {
      return toasts;
    },

    show(text: string, type: "info" | "success" | "error" = "info") {
      const id = nextId++;
      toasts = [...toasts, { id, text, type }];
      setTimeout(() => {
        toasts = toasts.filter((t) => t.id !== id);
      }, 5000);
    },
  };
}

export const toastStore = createToastStore();
